from collections import deque
from typing import TYPE_CHECKING, List, Optional, Tuple
import weakref

from pyglet.media.drivers.base import AbstractAudioDriver, AbstractWorkableAudioPlayer, MediaEvent
from pyglet.media.drivers.listener import AbstractListener
from pyglet.media.drivers.openal import interface
from pyglet.media.player_worker_thread import PlayerWorkerThread
from pyglet.util import debug_print

if TYPE_CHECKING:
    from pyglet.media import Source, Player


_debug = debug_print('debug_media')


class OpenALDriver(AbstractAudioDriver):
    def __init__(self, device_name: Optional[str] = None) -> None:
        super().__init__()

        self.device = interface.OpenALDevice(device_name)
        self.context = self.device.create_context()
        self.context.make_current()

        self._listener = OpenALListener(self)

        self.worker = PlayerWorkerThread()
        self.worker.start()

    def create_audio_player(self, source: 'Source', player: 'Player') -> 'OpenALAudioPlayer':
        assert self.device is not None, "Device was closed"
        return OpenALAudioPlayer(self, source, player)

    def delete(self) -> None:
        assert _debug("Delete OpenALDriver")
        self.worker.stop()
        self.context.delete_sources()
        self.device.buffer_pool.delete()
        self.context.delete()
        self.device.close()

    def have_version(self, major: int, minor: int) -> bool:
        return (major, minor) <= self.get_version()

    def get_version(self) -> Tuple[int, int]:
        assert self.device is not None, "Device was closed"
        return self.device.get_version()

    def get_extensions(self) -> List[str]:
        assert self.device is not None, "Device was closed"
        return self.device.get_extensions()

    def have_extension(self, extension: str) -> bool:
        return extension in self.get_extensions()

    def get_listener(self) -> 'OpenALListener':
        return self._listener


class OpenALListener(AbstractListener):
    def __init__(self, driver: 'OpenALDriver') -> None:
        self._driver = weakref.proxy(driver)
        self._al_listener = interface.OpenALListener()

    def _set_volume(self, volume: float) -> None:
        self._al_listener.gain = volume
        self._volume = volume

    def _set_position(self, position: Tuple[float, float, float]) -> None:
        self._al_listener.position = position
        self._position = position

    def _set_forward_orientation(self, orientation: Tuple[float, float, float]) -> None:
        self._al_listener.orientation = orientation + self._up_orientation
        self._forward_orientation = orientation

    def _set_up_orientation(self, orientation: Tuple[float, float, float]) -> None:
        self._al_listener.orientation = self._forward_orientation + orientation
        self._up_orientation = orientation


class OpenALAudioPlayer(AbstractWorkableAudioPlayer):
    COMFORTABLE_BUFFER_LIMIT = 1.0
    """How much unplayed data should be queued on the source through various
    buffers at all times, in seconds.
    Once this value is understepped, more data will be requested and added.
    """

    def __init__(self, driver: 'OpenALDriver', source: 'Source', player: 'Player') -> None:
        super().__init__(source, player)
        self.driver = driver
        self.alsource = driver.context.create_source()

        # Cursor positions, like DSound and Pulse drivers, refer to a
        # hypothetical infinite-length buffer.  Cursor units are in bytes.

        # The following should be true at all times:
        # buffer <= play <= write; buffer >= 0; play >= 0; write >= 0

        # Start of the current (head) AL buffer
        self._buffer_cursor = 0

        # Estimated playback cursor position (last seen)
        self._play_cursor = 0

        # Cursor position of end of the last queued AL buffer.
        self._write_cursor = 0

        # Whether the source has been exhausted of all data.
        # Don't bother trying to refill then and brace for eos.
        self._pyglet_source_exhausted = False

        # Whether the OpenAL source has played to its end.
        # Prevent duplicate dispatches of on_eos events.
        self._has_underrun = False

        # Deque of the currently queued buffer's sizes
        self._queued_buffer_sizes = deque()

        # Request ~500ms of audio data per refill
        fmt = source.audio_format
        self._ideal_buffer_size = fmt.align(int(fmt.bytes_per_second * 0.5))

    def delete(self) -> None:
        self.driver.worker.remove(self)
        self.alsource.delete()
        self.alsource = None

    def play(self) -> None:
        assert _debug('OpenALAudioPlayer.play()')

        assert self.driver is not None
        assert self.alsource is not None

        if not self.alsource.is_playing:
            self.alsource.play()

        self.driver.worker.add(self)

    def stop(self) -> None:
        self.driver.worker.remove(self)
        assert _debug('OpenALAudioPlayer.stop()')
        assert self.driver is not None
        assert self.alsource is not None
        self.alsource.pause()

    def clear(self) -> None:
        assert _debug('OpenALAudioPlayer.clear()')

        assert self.driver is not None
        assert self.alsource is not None

        super().clear()
        self.alsource.stop()
        self.alsource.clear()

        self._buffer_cursor = 0
        self._play_cursor = 0
        self._write_cursor = 0
        self._pyglet_source_exhausted = False
        self._has_underrun = False
        self._queued_buffer_sizes.clear()

    def _check_processed_buffers(self) -> None:
        buffers_processed = self.alsource.unqueue_buffers()
        for _ in range(buffers_processed):
            # Buffers have been processed (and already been removed from the ALSource);
            # Adjust buffer cursor.
            self._buffer_cursor += self._queued_buffer_sizes.popleft()

    def _update_play_cursor(self) -> None:
        self._play_cursor = self._buffer_cursor + self.alsource.byte_offset

    def work(self) -> None:
        self._check_processed_buffers()
        self._update_play_cursor()
        self.dispatch_media_events(self._play_cursor)

        if self._pyglet_source_exhausted:
            if not self._has_underrun and not self.alsource.is_playing:
                self._has_underrun = True
                assert _debug("OpenALAudioPlayer: Dispatching eos")
                MediaEvent('on_eos').sync_dispatch_to_player(self.player)
        else:
            refilled = False
            while self._should_refill():
                self._refill()
                refilled = True

            if refilled and not self.alsource.is_playing:
                # Very unlikely case where the refill was delayed by so much the
                # source underran and stopped. If it did, restart it.
                self.alsource.play()

    def _should_refill(self) -> bool:
        if self._pyglet_source_exhausted:
            return False

        remaining_bytes = self._write_cursor - self._play_cursor
        bytes_per_second = self.source.audio_format.bytes_per_second
        return remaining_bytes / bytes_per_second < self.COMFORTABLE_BUFFER_LIMIT

    def get_time(self) -> float:
        return self._play_cursor / self.source.audio_format.bytes_per_second

    def _refill(self) -> None:
        compensation_time = self.get_audio_time_diff()
        audio_data = self.source.get_audio_data(self._ideal_buffer_size, compensation_time)
        if audio_data is None:
            self._pyglet_source_exhausted = True
            # We could schedule the on_eos event at the very end of written data here, but
            # this would not check whether the source actually stopped playing.
            # Of course logic dictates if it reports the play position being into it as far
            # as its last buffer's length, it's unlikely to be playing anymore, but just be extra
            # safe and dispatch it explicitly in `work`
            # self._events.append((self._write_cursor, MediaEvent('on_eos')))
            return

        # We got new audio data; first queue its events
        self.append_events(self._write_cursor, audio_data.events)

        refill_length = audio_data.length

        # Get, fill and queue OpenAL buffer using the entire AudioData
        buf = self.alsource.get_buffer()
        buf.data(audio_data, self.source.audio_format, refill_length)
        self.alsource.queue_buffer(buf)

        # Adjust the write cursor and memorize buffer length
        self._write_cursor += refill_length
        self._queued_buffer_sizes.append(refill_length)

    def prefill_audio(self) -> None:
        while self._should_refill():
            self._refill()

    def set_volume(self, volume: float) -> None:
        self.alsource.gain = volume

    def set_position(self, position: Tuple[float, float, float]) -> None:
        self.alsource.position = position

    def set_min_distance(self, min_distance: float) -> None:
        self.alsource.reference_distance = min_distance

    def set_max_distance(self, max_distance: float) -> None:
        self.alsource.max_distance = max_distance

    def set_pitch(self, pitch: float) -> None:
        self.alsource.pitch = pitch

    def set_cone_orientation(self, cone_orientation: Tuple[float, float, float]) -> None:
        self.alsource.direction = cone_orientation

    def set_cone_inner_angle(self, cone_inner_angle: float) -> None:
        self.alsource.cone_inner_angle = cone_inner_angle

    def set_cone_outer_angle(self, cone_outer_angle: float) -> None:
        self.alsource.cone_outer_angle = cone_outer_angle

    def set_cone_outer_gain(self, cone_outer_gain: float) -> None:
        self.alsource.cone_outer_gain = cone_outer_gain
