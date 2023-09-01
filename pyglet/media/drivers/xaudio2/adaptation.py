from collections import deque
import math
from typing import Deque, Tuple, TYPE_CHECKING

from pyglet.media.drivers.base import AbstractAudioDriver, AbstractAudioPlayer, MediaEvent
from pyglet.media.player_worker_thread import PlayerWorkerThread
from pyglet.media.drivers.listener import AbstractListener
from pyglet.util import debug_print
from . import interface

if TYPE_CHECKING:
    from pyglet.media.codecs import AudioData, Source
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
        return XAudio2AudioPlayer(self, self._xa2_driver, source, player)

    def get_listener(self) -> 'XAudio2Listener':
        return self._listener

    def delete(self) -> None:
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
        self._xa2_listener.orientation = _convert_coordinates(self._forward_orientation) + _convert_coordinates(
            self._up_orientation)


class XAudio2AudioPlayer(AbstractAudioPlayer):
    max_buffer_count = 3  # Max in queue at once, increasing may impact performance depending on buffer size.

    def __init__(self, driver, xa2_driver, source: 'Source', player: 'Player') -> None:
        super().__init__(source, player)
        # We keep here a strong reference because the AudioDriver is anyway
        # a singleton object which will only be deleted when the application
        # shuts down. The AudioDriver does not keep a ref to the AudioPlayer.
        self.driver = driver
        self._xa2_driver = xa2_driver

        # If cleared, we need to check when it's done clearing.
        self._flushing = False
        self._delete_after_flush = False

        # Need to cache these because pyglet API allows update separately, but
        # DSound requires both to be set at once.
        self._cone_inner_angle = 360
        self._cone_outer_angle = 360

        # Desired play state (may be actually paused due to underrun -- not
        # implemented yet).
        self._playing = False

        # Theoretical write and play cursors for an infinite buffer.  play
        # cursor is always <= write cursor (when equal, underrun is
        # happening).
        self._write_cursor = 0
        self._play_cursor = 0

        self._audio_data_in_use: Deque['AudioData'] = deque()
        self._pyglet_source_exhausted = False

        self._xa2_source_voice = self._xa2_driver.get_source_voice(source.audio_format, self)

        self._ideal_buffer_size = self.source.audio_format.align(int(source.audio_format.bytes_per_second * 0.5))

    def on_driver_destroy(self) -> None:
        self.stop()
        self._xa2_source_voice = None

    def on_driver_reset(self) -> None:
        self._xa2_source_voice = self._xa2_driver.get_source_voice(self.source, self)

        # Queue up any buffers that are still in queue but weren't deleted. This does not
        # pickup where the last sample played, only where the last buffer was submitted.
        # As such, audio will be replayed.
        # TODO: Make best effort by using XAUDIO2_BUFFER.PlayBegin in conjunction
        # with last playback sample
        for cx2_buffer in self._buffers:
            self._xa2_source_voice.submit_buffer(cx2_buffer)

    def _flush(self) -> None:
        self.stop()
        if not self._flushing:
            self._xa2_source_voice.flush()
            if self._xa2_source_voice.buffers_queued > 0:
                self._flushing = True

    def delete(self) -> None:
        self._delete_after_flush = True
        self._flush()

    def play(self) -> None:
        assert _debug(f'XAudio2 play: {self._playing=}, {self._flushing=}')

        if not self._playing:
            self._playing = True
            if not self._flushing:
                self.driver.worker.add(self)
                self._xa2_source_voice.play()

        assert _debug('return XAudio2 play')

    def stop(self) -> None:
        assert _debug('XAudio2 stop')

        self.driver.worker.remove(self)
        if self._playing:
            self._playing = False
            self._xa2_source_voice.stop()

        assert _debug('return XAudio2 stop')

    def clear(self) -> None:
        assert _debug('XAudio2 clear')
        super().clear()
        self._play_cursor = 0
        self._write_cursor = 0
        self._pyglet_source_exhausted = False
        self._flush()

    def on_buffer_end(self, buffer_context_ptr: int) -> None:
        # A buffer stopped being played by the voice.
        # Assume it's the first one; this may not exactly hold for the calls
        # produced as consequence of `flush`.
        assert self._audio_data_in_use
        self._audio_data_in_use.popleft()
        # This should cause the AudioData to lose all its references and be gc'd

        if self._audio_data_in_use:
            assert _debug("Buffer ended, but another remains")
            return

        assert self._xa2_source_voice.buffers_queued == 0

        if self._flushing:
            assert _debug("Last buffer ended after flush")
            # We were emptying buffers due to a flush;
            # source is now truly empty and can be reused/returned
            self._flushing = False
            if self._delete_after_flush:
                assert _debug("  Returning voice")
                self._xa2_driver.return_voice(self._xa2_source_voice)
                self._xa2_driver = None
                self._xa2_source_voice = None
        else:
            assert _debug("Last buffer ended normally")
            # Last buffer ran out naturally, out of AudioData; voice will now fall silent
            if self._pyglet_source_exhausted:
                MediaEvent('on_eos').sync_dispatch_to_player(self.player)
            else:
                # Shouldn't have ran out; supplier is running behind
                # All we can do is wait; as long as voices are not stopped via `Stop`, they will
                # immediately continue playing the new buffer once it arrives
                pass

    def _refill(self) -> None:
        compensation_time = self.get_audio_time_diff()
        audio_data = self.source.get_audio_data(self._ideal_buffer_size, compensation_time)
        if audio_data is None:
            self._pyglet_source_exhausted = True
            return

        assert _debug(f"Pushed buffer of size {audio_data.length}")
        xa2_buffer = self._xa2_driver.create_buffer(audio_data)
        self._audio_data_in_use.append(audio_data)
        self._xa2_source_voice.submit_buffer(xa2_buffer)

        self.append_events(self._write_cursor, audio_data.events)
        self._write_cursor += audio_data.length

    def work(self) -> None:
        if self._flushing:
            return

        while self._needs_refill():
            self._refill()

    def _needs_refill(self) -> bool:
        return not self._pyglet_source_exhausted and len(self._audio_data_in_use) < 3

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
