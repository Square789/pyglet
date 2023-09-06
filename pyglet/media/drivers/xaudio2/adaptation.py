from collections import deque
from enum import IntEnum
import math
import threading
from typing import Deque, Tuple, TYPE_CHECKING

from pyglet.media.drivers.base import AbstractAudioDriver, AbstractAudioPlayer, MediaEvent
from pyglet.media.player_worker_thread import PlayerWorkerThread
from pyglet.media.drivers.listener import AbstractListener
from pyglet.util import debug_print
from . import interface

if TYPE_CHECKING:
    from pyglet.media.codecs import AudioData, AudioFormat, Source
    from pyglet.media.player import Player


_debug = debug_print('debug_media')


def _convert_coordinates(coordinates: Tuple[float, float, float]) -> Tuple[float, float, float]:
    x, y, z = coordinates
    return x, y, -z


class XAudio2Driver(AbstractAudioDriver):
    def __init__(self) -> None:
        self._xa2_driver = interface.XAudio2Driver()
        self._xa2_listener = self._xa2_driver.create_listener()
        self._listener = XAudio2Listener(self._xa2_listener, self._xa2_driver)

        self.worker = PlayerWorkerThread()
        self.worker.start()

    def get_performance(self) -> interface.lib.XAUDIO2_PERFORMANCE_DATA:
        assert self._xa2_driver is not None
        return self._xa2_driver.get_performance()

    def create_audio_player(self, source: 'Source', player: 'Player') -> 'XAudio2AudioPlayer':
        assert self._xa2_driver is not None
        return XAudio2AudioPlayer(self, source, player)

    def get_listener(self) -> 'XAudio2Listener':
        return self._listener

    def delete(self) -> None:
        self.worker.stop()
        self.worker = None
        self._xa2_driver._delete_driver()
        self._xa2_driver = None
        self._xa2_listener = None


class XAudio2Listener(AbstractListener):
    def __init__(self, xa2_listener, xa2_driver) -> None:
        self._xa2_listener = xa2_listener
        self._xa2_driver = xa2_driver

    def _set_volume(self, volume: float) -> None:
        self._volume = volume
        self._xa2_driver.volume = volume

    def _set_position(self, position: Tuple[float, float, float]) -> None:
        self._position = position
        self._xa2_listener.position = _convert_coordinates(position)

    def _set_forward_orientation(self, orientation: Tuple[float, float, float]) -> None:
        self._forward_orientation = orientation
        self._set_orientation()

    def _set_up_orientation(self, orientation: Tuple[float, float, float]) -> None:
        self._up_orientation = orientation
        self._set_orientation()

    def _set_orientation(self) -> None:
        self._xa2_listener.orientation = (
            _convert_coordinates(self._forward_orientation) +
            _convert_coordinates(self._up_orientation))


class FlushOperation(IntEnum):
    FLUSH = 0
    FLUSH_THEN_DELETE = 1


class XAudio2AudioPlayer(AbstractAudioPlayer):
    def __init__(self, driver: 'XAudio2Driver', source: 'Source', player: 'Player') -> None:
        super().__init__(source, player)
        # We keep here a strong reference because the AudioDriver is anyway
        # a singleton object which will only be deleted when the application
        # shuts down. The AudioDriver does not keep a ref to the AudioPlayer.
        self.driver = driver

        self._flush_operation = None

        # Need to cache these because pyglet API allows update separately, but
        # DSound requires both to be set at once.
        self._cone_inner_angle = 360
        self._cone_outer_angle = 360

        # Desired play state. (`True` doesn't necessarily mean the player is playing.
        # It may be silent due to either underrun or because a flush is in progress.)
        self._playing = False

        # Theoretical write and play cursors for an infinite buffer.  play
        # cursor is always <= write cursor (when equal, underrun is
        # happening).
        self._write_cursor = 0
        self._play_cursor = 0

        self._audio_data_in_use: Deque['AudioData'] = deque()
        self._pyglet_source_exhausted = False

        # A lock to be held whenever modifying things relating to the in-use audio data and
        # flush operation. Ensures that the XAudio2 callbacks will not interfere with the
        # player operations.
        self._lock = threading.Lock()

        self._xa2_source_voice = self.driver._xa2_driver.get_source_voice(source.audio_format, self)

        # About 1.5s of audio data buffered at all times, like other backends.
        self.max_buffer_count = 3
        self._buffer_size = self.source.audio_format.align(
            int(source.audio_format.bytes_per_second * 0.5))

    def on_driver_destroy(self) -> None:
        self.stop()
        self._xa2_source_voice = None

    def on_driver_reset(self) -> None:
        self._xa2_source_voice = self.driver._xa2_driver.get_source_voice(self.source, self)

        # Queue up any buffers that are still in queue but weren't deleted. This does not
        # pickup where the last sample played, only where the last buffer was submitted.
        # As such, audio will be replayed.
        # TODO: Make best effort by using XAUDIO2_BUFFER.PlayBegin in conjunction
        # with last playback sample
        for audio_data in self._audio_data_in_use:
            xa2_buffer = interface.create_xa2_buffer(audio_data)
            self._xa2_source_voice.submit_buffer(xa2_buffer)

    def _on_flush_complete(self) -> None:
        # Remember to hold the lock when calling this
        assert not self._audio_data_in_use

        op = self._flush_operation
        self._flush_operation = None

        if op is FlushOperation.FLUSH:
            # Before a flush, the voice is always stopped, so play if _playing is True.
            # Otherwise nothing has to be done
            if self._playing:
                self._xa2_source_voice.play()
                self.driver.worker.add(self)

        elif op is FlushOperation.FLUSH_THEN_DELETE:
            self._delete_now()

    def _flush(self, operation: FlushOperation) -> None:
        # Remember to hold the lock when calling this
        assert _debug(f"XAudio2 _flush: {operation=}")

        if self._flush_operation is None:
            self.stop()
            self._flush_operation = operation
            self._xa2_source_voice.flush()
            if self._xa2_source_voice.buffers_queued == 0:
                self._on_flush_complete()
        else:
            self._flush_operation = max(self._flush_operation, operation)

        assert _debug("return XAudio2 _flush")

    def _delete_now(self) -> None:
        assert _debug("XAudio2: Player deleted, returning voice")
        self.driver._xa2_driver.return_voice(self._xa2_source_voice)
        self.driver = None
        self._xa2_source_voice = None
        self._audio_data_in_use.clear()

    def delete(self) -> None:
        assert _debug("Xaudio2 delete")
        if self.driver._xa2_driver is None:
            # Driver was deleted, voice is gone; just break up some references and return
            self.driver = None
            self._xa2_source_voice = None
            self._audio_data_in_use.clear()
            return

        self.driver.worker.remove(self)
        with self._lock:
            self._flush(FlushOperation.FLUSH_THEN_DELETE)

    def play(self) -> None:
        self._lock.acquire()
        assert _debug(f'XAudio2 play: {self._playing=}, {self._flush_operation=}')

        if not self._playing:
            self._playing = True
            if self._flush_operation is None:
                self._xa2_source_voice.play()
                self._lock.release()
                self.driver.worker.add(self)
                return

        self._lock.release()

        assert _debug('return XAudio2 play')

    def stop(self) -> None:
        assert _debug('XAudio2 stop')

        if self._playing:
            self.driver.worker.remove(self)
            self._playing = False
            self._xa2_source_voice.stop()

        assert _debug('return XAudio2 stop')

    def clear(self) -> None:
        assert _debug('XAudio2 clear')
        super().clear()
        with self._lock:
            self._play_cursor = 0
            self._write_cursor = 0
            self._pyglet_source_exhausted = False
            self._flush(FlushOperation.FLUSH)

    def on_buffer_end(self, buffer_context_ptr: int) -> None:
        # Called from the XAudio2 thread.
        # A buffer stopped being played by the voice.
        # Assume it's the first one; although this may not exactly hold for the calls
        # produced as consequence of `flush`.
        with self._lock:
            assert self._audio_data_in_use
            self._audio_data_in_use.popleft()
            # This should cause the AudioData to lose all its references and be gc'd

            if self._audio_data_in_use:
                assert _debug(f"Buffer ended, others remain: {len(self._audio_data_in_use)=}")
                return

            assert self._xa2_source_voice.buffers_queued == 0

            if self._flush_operation is not None:
                assert _debug("Last buffer ended after flush")
                # We were emptying buffers due to a flush;
                # source is now truly empty and can be reused/returned
                self._on_flush_complete()
            else:
                # Last buffer ran out naturally, out of AudioData; voice will now fall silent
                if self._pyglet_source_exhausted:
                    assert _debug("Last buffer ended normally, dispatching eos")
                    MediaEvent('on_eos').sync_dispatch_to_player(self.player)
                else:
                    assert _debug("Last buffer ended normally, source is lagging behind")
                    # Shouldn't have ran out; supplier is running behind
                    # All we can do is wait; as long as voices are not stopped via `Stop`, they will
                    # immediately continue playing the new buffer once it arrives
                    pass

    def _refill(self) -> None:
        """Get one piece of AudioData and submit it to the voice.
        This method will release the lock around the call to `get_audio_data`,
        so make sure it's held upon calling.
        """
        assert _debug(f"XAudio2: Retrieving new buffer")
        compensation_time = self.get_audio_time_diff()

        self._lock.release()
        audio_data = self.source.get_audio_data(self._buffer_size, compensation_time)
        self._lock.acquire()

        if audio_data is None:
            assert _debug(f"XAudio2: Source is out of data")
            self._pyglet_source_exhausted = True
            if not self._audio_data_in_use:
                MediaEvent('on_eos').sync_dispatch_to_player(self.player)
            return

        xa2_buffer = interface.create_xa2_buffer(audio_data)
        self._audio_data_in_use.append(audio_data)
        self._xa2_source_voice.submit_buffer(xa2_buffer)
        assert _debug(f"XAudio2: Submitted buffer of size {audio_data.length}")

        self.append_events(self._write_cursor, audio_data.events)
        self._write_cursor += audio_data.length

    def work(self) -> None:
        with self._lock:
            # TODO: Get playback time and dispatch events?
            if self._flush_operation is not None:
                return

            while self._needs_refill():
                self._refill()

    def _needs_refill(self) -> bool:
        return (not self._pyglet_source_exhausted and
                len(self._audio_data_in_use) < self.max_buffer_count)

    def prefill_audio(self) -> None:
        self.work()

    def get_time(self) -> float:
        pass
        # TODO look into after deciding what to do for llp time

    def set_volume(self, volume: float) -> None:
        self._xa2_source_voice.volume = volume

    def set_position(self, position: Tuple[float, float, float]) -> None:
        if self._xa2_source_voice.is_emitter:
            self._xa2_source_voice.position = _convert_coordinates(position)

    def set_min_distance(self, min_distance: float) -> None:
        """Not a true min distance, but similar effect. Changes CurveDistanceScaler default is 1."""
        if self._xa2_source_voice.is_emitter:
            self._xa2_source_voice.distance_scaler = min_distance

    def set_max_distance(self, max_distance: float) -> None:
        """No such thing built into xaudio2"""
        return

    def set_pitch(self, pitch: float) -> None:
        self._xa2_source_voice.frequency = pitch

    def set_cone_orientation(self, cone_orientation: Tuple[float, float, float]) -> None:
        if self._xa2_source_voice.is_emitter:
            self._xa2_source_voice.cone_orientation = _convert_coordinates(cone_orientation)

    def set_cone_inner_angle(self, cone_inner_angle: float) -> None:
        if self._xa2_source_voice.is_emitter:
            self._cone_inner_angle = int(cone_inner_angle)
            self._set_cone_angles()

    def set_cone_outer_angle(self, cone_outer_angle: float) -> None:
        if self._xa2_source_voice.is_emitter:
            self._cone_outer_angle = int(cone_outer_angle)
            self._set_cone_angles()

    def _set_cone_angles(self) -> None:
        inner = min(self._cone_inner_angle, self._cone_outer_angle)
        outer = max(self._cone_inner_angle, self._cone_outer_angle)
        self._xa2_source_voice.set_cone_angles(math.radians(inner), math.radians(outer))

    def set_cone_outer_gain(self, cone_outer_gain: float) -> None:
        if self._xa2_source_voice.is_emitter:
            self._xa2_source_voice.cone_outside_volume = cone_outer_gain
