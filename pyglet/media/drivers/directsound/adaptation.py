import math
import ctypes

from . import interface
from pyglet.media.drivers.base import AbstractAudioDriver, AbstractWorkableAudioPlayer, MediaEvent
from pyglet.media.drivers.listener import AbstractListener
from pyglet.media.player_worker_thread import PlayerWorkerThread
from pyglet.util import debug_print

_debug = debug_print('debug_media')


def _convert_coordinates(coordinates):
    x, y, z = coordinates
    return x, y, -z


def _gain2db(gain):
    """
    Convert linear gain in range [0.0, 1.0] to 100ths of dB.

    Power gain = P1/P2
    dB = 2 log(P1/P2)
    dB * 100 = 1000 * log(power gain)
    """
    if gain <= 0:
        return -10000
    return max(-10000, min(int(1000 * math.log2(min(gain, 1))), 0))


def _db2gain(db):
    """Convert 100ths of dB to linear gain."""
    return math.pow(10.0, float(db)/1000.0)


class DirectSoundAudioPlayer(AbstractWorkableAudioPlayer):
    # Need to cache these because pyglet API allows update separately, but
    # DSound requires both to be set at once.
    _cone_inner_angle = 360
    _cone_outer_angle = 360

    min_buffer_size = 9600

    def __init__(self, driver, ds_driver, source, player):
        super(DirectSoundAudioPlayer, self).__init__(source, player)

        # We keep here a strong reference because the AudioDriver is anyway
        # a singleton object which will only be deleted when the application
        # shuts down. The AudioDriver does not keep a ref to the AudioPlayer.
        self.driver = driver
        self._ds_driver = ds_driver

        # Desired play state (may be actually paused due to underrun -- not
        # implemented yet).
        self._playing = False

        # Indexes into DSound circular buffer.  Complications ensue wrt each
        # other to avoid writing over the play cursor.  See _get_write_size and
        # write().
        self._play_cursor_ring = 0
        self._write_cursor_ring = 0

        # Theoretical write and play cursors for an infinite buffer.  play
        # cursor is always <= write cursor (when equal, underrun is
        # happening).
        self._write_cursor = 0
        self._play_cursor = 0

        # Cursor position of where the source ran out.
        # We are done once the play cursor crosses it.
        self._eos_cursor = None

        # Whether the source has hit its end; protect against duplicate
        # dispatch of on_eos events.
        self._has_underrun = False

        # DSound buffer
        self._ds_buffer = self._ds_driver.create_buffer(source.audio_format)
        self._buffer_size = self._ds_buffer.buffer_size
        # Align to multiple of 2 temporarily. Will be fixed again before this branch
        # ever goes anywhere though
        self._tolerable_empty_space = (self._buffer_size // 3) & 0xFFFFFFFE

        self._ds_buffer.current_position = 0

        self._refill(self._buffer_size)

    def __del__(self):
        # We decrease the IDirectSound refcount
        self.driver._ds_driver._native_dsound.Release()

    def delete(self):
        self.driver.worker.remove(self)

    def play(self):
        assert _debug('DirectSound play')

        if not self._playing:
            self._playing = True
            self._ds_buffer.play()

        self.driver.worker.add(self)
        assert _debug('return DirectSound play')

    def stop(self):
        assert _debug('DirectSound stop')
        self.driver.worker.remove(self)

        if self._playing:
            self._playing = False
            self._ds_buffer.stop()

        assert _debug('return DirectSound stop')

    def clear(self):
        assert _debug('DirectSound clear')
        super(DirectSoundAudioPlayer, self).clear()
        self._ds_buffer.current_position = 0
        self._play_cursor_ring = self._write_cursor_ring = 0
        self._play_cursor = self._write_cursor
        self._eos_cursor = None
        self._has_underrun = False

    def get_time(self):
        return self._play_cursor / self.source.audio_format.bytes_per_second

    def work(self):
        self._update_play_cursor()
        self.dispatch_media_events(self._play_cursor)

        if self._eos_cursor is not None:
            # Source exhausted, waiting for play cursor to hit the end
            if not self._has_underrun and self._play_cursor > self._eos_cursor:
                self._has_underrun = True
                MediaEvent('on_eos').sync_dispatch_to_player(self.player)
            # While we are still playing / waiting for the on_eos to be dispatched for
            # the player to stop; the buffer continues playing. Ensure that silence is
            # filled.
            if (empty := self._get_empty_buffer_space()) > self._tolerable_empty_space:
                self.write(None, empty)
        else:
            self._maybe_fill()

    def _maybe_fill(self):
        if (empty := self._get_empty_buffer_space()) > self._tolerable_empty_space:
            self._refill(empty)

    def _refill(self, size):
        compensation_time = self.get_audio_time_diff()
        audio_data = self.source.get_audio_data(size, compensation_time)
        if audio_data is None:
            assert _debug('DirectSoundAudioPlayer: Out of audio data')
            rlen = 0
            if self._eos_cursor is None:
                self._eos_cursor = self._write_cursor
        else:
            assert _debug(f'DirectSoundAudioPlayer: Got {audio_data.length} bytes of audio data')
            rlen = audio_data.length
            self.append_events(self._write_cursor, audio_data.events)
            self.write(audio_data, audio_data.length)

        if rlen < size:
            self.write(None, size - rlen)

    def _update_play_cursor(self):
        play_cursor_ring = self._ds_buffer.current_position.play_cursor
        if play_cursor_ring < self._play_cursor_ring:
            # Wrapped around
            self._play_cursor += self._buffer_size - self._play_cursor_ring
            self._play_cursor += play_cursor_ring
        else:
            self._play_cursor += play_cursor_ring - self._play_cursor_ring
        self._play_cursor_ring = play_cursor_ring

    def _get_empty_buffer_space(self):
        return self._buffer_size - max(self._write_cursor - self._play_cursor, 0)

    def write(self, audio_data, length):
        assert _debug(f'Writing {length} bytes of {"silence" if audio_data is None else "data"}')
        if length == 0:
            return

        write_ptr = self._ds_buffer.lock(self._write_cursor_ring, length)
        assert 0 < length <= self._buffer_size
        assert length == write_ptr.audio_length_1.value + write_ptr.audio_length_2.value

        if audio_data is None:
            # Write silence
            c = 0x80 if self.source.audio_format.sample_size == 8 else 0
            ctypes.memset(write_ptr.audio_ptr_1, c, write_ptr.audio_length_1.value)
            if write_ptr.audio_length_2.value > 0:
                ctypes.memset(write_ptr.audio_ptr_2, c, write_ptr.audio_length_2.value)
        else:
            ctypes.memmove(write_ptr.audio_ptr_1, audio_data.data, write_ptr.audio_length_1.value)
            audio_data.consume(write_ptr.audio_length_1.value, self.source.audio_format)
            if write_ptr.audio_length_2.value > 0:
                ctypes.memmove(write_ptr.audio_ptr_2, audio_data.data, write_ptr.audio_length_2.value)
                audio_data.consume(write_ptr.audio_length_2.value, self.source.audio_format)
        self._ds_buffer.unlock(write_ptr)

        self._write_cursor += length
        self._write_cursor_ring += length
        self._write_cursor_ring %= self._buffer_size

    def set_volume(self, volume):
        self._ds_buffer.volume = _gain2db(volume)

    def set_position(self, position):
        if self._ds_buffer.is3d:
            self._ds_buffer.position = _convert_coordinates(position)

    def set_min_distance(self, min_distance):
        if self._ds_buffer.is3d:
            self._ds_buffer.min_distance = min_distance

    def set_max_distance(self, max_distance):
        if self._ds_buffer.is3d:
            self._ds_buffer.max_distance = max_distance

    def set_pitch(self, pitch):
        frequency = int(pitch * self.source.audio_format.sample_rate)
        self._ds_buffer.frequency = frequency

    def set_cone_orientation(self, cone_orientation):
        if self._ds_buffer.is3d:
            self._ds_buffer.cone_orientation = _convert_coordinates(cone_orientation)

    def set_cone_inner_angle(self, cone_inner_angle):
        if self._ds_buffer.is3d:
            self._cone_inner_angle = int(cone_inner_angle)
            self._set_cone_angles()

    def set_cone_outer_angle(self, cone_outer_angle):
        if self._ds_buffer.is3d:
            self._cone_outer_angle = int(cone_outer_angle)
            self._set_cone_angles()

    def _set_cone_angles(self):
        inner = min(self._cone_inner_angle, self._cone_outer_angle)
        outer = max(self._cone_inner_angle, self._cone_outer_angle)
        self._ds_buffer.set_cone_angles(inner, outer)

    def set_cone_outer_gain(self, cone_outer_gain):
        if self._ds_buffer.is3d:
            volume = _gain2db(cone_outer_gain)
            self._ds_buffer.cone_outside_volume = volume

    def prefill_audio(self):
        self._maybe_fill()


class DirectSoundDriver(AbstractAudioDriver):
    def __init__(self):
        self._ds_driver = interface.DirectSoundDriver()
        self._ds_listener = self._ds_driver.create_listener()

        assert self._ds_driver is not None
        assert self._ds_listener is not None

        self.worker = PlayerWorkerThread()
        self.worker.start()

    def __del__(self):
        self.delete()

    def create_audio_player(self, source, player):
        assert self._ds_driver is not None
        # We increase IDirectSound refcount for each AudioPlayer instantiated
        # This makes sure the AudioPlayer still has a valid _native_dsound to
        # clean-up itself during tear-down.
        self._ds_driver._native_dsound.AddRef()
        return DirectSoundAudioPlayer(self, self._ds_driver, source, player)

    def get_listener(self):
        assert self._ds_driver is not None
        assert self._ds_listener is not None
        return DirectSoundListener(self._ds_listener, self._ds_driver.primary_buffer)

    def delete(self):
        if hasattr(self, 'worker'):
            self.worker.stop()
        # Make sure the _ds_listener is deleted before the _ds_driver
        self._ds_listener = None


class DirectSoundListener(AbstractListener):
    def __init__(self, ds_listener, ds_buffer):
        self._ds_listener = ds_listener
        self._ds_buffer = ds_buffer

    def _set_volume(self, volume):
        self._volume = volume
        self._ds_buffer.volume = _gain2db(volume)

    def _set_position(self, position):
        self._position = position
        self._ds_listener.position = _convert_coordinates(position)

    def _set_forward_orientation(self, orientation):
        self._forward_orientation = orientation
        self._set_orientation()

    def _set_up_orientation(self, orientation):
        self._up_orientation = orientation
        self._set_orientation()

    def _set_orientation(self):
        self._ds_listener.orientation = (_convert_coordinates(self._forward_orientation)
                                         + _convert_coordinates(self._up_orientation))
