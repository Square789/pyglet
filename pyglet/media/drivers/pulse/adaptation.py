from typing import Optional, TYPE_CHECKING
import weakref

from pyglet.media.drivers.base import AbstractAudioDriver, AbstractAudioPlayer, MediaEvent
from pyglet.media.drivers.listener import AbstractListener
from pyglet.util import debug_print

from . import lib_pulseaudio as pa
from .interface import PulseAudioMainloop

if TYPE_CHECKING:
    from pyglet.media.codecs import AudioData, Source
    from pyglet.media.player import Player



_debug = debug_print('debug_media')


class PulseAudioDriver(AbstractAudioDriver):
    def __init__(self) -> None:
        self.mainloop = PulseAudioMainloop()
        self.mainloop.start()
        self.lock = self.mainloop
        self.context = None

        self._players = weakref.WeakSet()
        self._listener = PulseAudioListener(self)

    def create_audio_player(self, source: 'Source', player: 'Player') -> 'PulseAudioPlayer':
        assert self.context is not None
        player = PulseAudioPlayer(source, player, self)
        self._players.add(player)
        return player

    def connect(self, server: Optional[bytes] = None) -> None:
        """Connect to pulseaudio server.

        :Parameters:
            `server` : bytes
                Server to connect to, or ``None`` for the default local
                server (which may be spawned as a daemon if no server is
                found).
        """
        # TODO disconnect from old
        assert not self.context, 'Already connected'

        self.context = self.mainloop.create_context()
        self.context.connect(server)

    def dump_debug_info(self):
        print('Client version: ', pa.pa_get_library_version())
        print('Server:         ', self.context.server)
        print('Protocol:       ', self.context.protocol_version)
        print('Server protocol:', self.context.server_protocol_version)
        print('Local context:  ', self.context.is_local and 'Yes' or 'No')

    def delete(self) -> None:
        """Completely shut down pulseaudio client."""
        if self.mainloop is not None:
            with self.mainloop.lock:
                if self.context is not None:
                    self.context.delete()
                    self.context = None

            self.mainloop.delete()
            self.mainloop = None
            self.lock = None

    def get_listener(self) -> 'PulseAudioListener':
        return self._listener


class PulseAudioListener(AbstractListener):
    def __init__(self, driver: 'PulseAudioDriver') -> None:
        self.driver = weakref.proxy(driver)

    def _set_volume(self, volume: float) -> None:
        self._volume = volume
        for player in self.driver._players:
            player.set_volume(player._volume)

    def _set_position(self, position):
        self._position = position

    def _set_forward_orientation(self, orientation):
        self._forward_orientation = orientation

    def _set_up_orientation(self, orientation):
        self._up_orientation = orientation


class PulseAudioPlayer(AbstractAudioPlayer):
    _volume = 1.0

    def __init__(self, source: 'Source', player: 'Player', driver: 'PulseAudioDriver') -> None:
        super(PulseAudioPlayer, self).__init__(source, player)
        self.driver = weakref.ref(driver)

        self._timestamps = []  # List of (ref_time, timestamp)
        self._write_index = 0  # Current write index (tracked manually)
        self._read_index_valid = False # True only if buffer has non-stale data

        self._clear_write = False
        self._buffered_audio_data = None
        self._playing = False

        self._current_audio_data = None

        self._time_sync_operation = None

        audio_format = source.audio_format
        assert audio_format

        with driver.mainloop.lock:
            self.stream = driver.context.create_stream(audio_format)
            self.stream.push_handlers(self)
            self.stream.connect_playback()
            assert self.stream.is_ready

        assert _debug('PulseAudioPlayer: __init__ finished')

    def on_write_needed(self, nbytes: int, underflow: bool) -> None:
        if underflow:
            self._handle_underflow()
        else:
            self._write_to_stream(nbytes)

        # Asynchronously update time
        if self._events:
            if self._time_sync_operation is not None and self._time_sync_operation.is_done:
                self._time_sync_operation.delete()
                self._time_sync_operation = None
            if self._time_sync_operation is None:
                assert _debug('PulseAudioPlayer: trigger timing info update')
                self._time_sync_operation = self.stream.update_timing_info(self._process_events)

    def _get_audio_data(self, nbytes: Optional[int] = None) -> 'AudioData':
        if self._current_audio_data is None and self.source is not None:
            # Always try to buffer at least 1 second of audio data
            min_bytes = 1 * self.source.audio_format.bytes_per_second
            if nbytes is None:
                nbytes = min_bytes
            else:
                nbytes = min(min_bytes, nbytes)
            assert _debug(f'PulseAudioPlayer: Try to get {nbytes} bytes of audio data')
            compensation_time = self.get_audio_time_diff()
            self._current_audio_data = self.source.get_audio_data(nbytes, compensation_time)
        if self._current_audio_data is None:
            assert _debug('PulseAudioPlayer: No audio data available')
        else:
            assert _debug(f'PulseAudioPlayer: Got {self._current_audio_data.length} bytes of audio data')
            self.append_events(self._write_index, self._current_audio_data.events)
        return self._current_audio_data

    def _has_audio_data(self) -> bool:
        return self._get_audio_data() is not None

    def _consume_audio_data(self, nbytes: int) -> None:
        if self._current_audio_data is not None:
            if nbytes == self._current_audio_data.length:
                self._current_audio_data = None
            else:
                self._current_audio_data.consume(nbytes, self.source.audio_format)

    def _write_to_stream(self, nbytes: Optional[int] = None) -> None:
        if nbytes is None:
            nbytes = self.stream.get_writable_size()
        assert _debug(f'PulseAudioPlayer: Requested to write {nbytes} bytes to stream')

        seek_mode = pa.PA_SEEK_RELATIVE
        if self._clear_write:
            # When seeking, the stream.writable_size will be 0.
            # So we force at least 4096 bytes to overwrite the Buffer
            # starting at read index
            nbytes = max(4096, nbytes)
            seek_mode = pa.PA_SEEK_RELATIVE_ON_READ
            self._clear_write = False
            assert _debug('PulseAudioPlayer: Clear buffer')

        while self._has_audio_data() and nbytes > 0:
            audio_data = self._get_audio_data()

            write_length = min(nbytes, audio_data.length)
            consumption = self.stream.write(audio_data, write_length, seek_mode)

            seek_mode = pa.PA_SEEK_RELATIVE
            self._read_index_valid = True
            self._timestamps.append((self._write_index, audio_data.timestamp))
            self._write_index += consumption

            assert _debug('PulseAudioPlayer: Actually wrote {} bytes '
                          'to stream'.format(consumption))
            self._consume_audio_data(consumption)

            nbytes -= consumption

        if not self._has_audio_data():
            # In case the source group wasn't long enough to prebuffer stream
            # to PA's satisfaction, trigger immediate playback (has no effect
            # if stream is already playing).
            if self._playing:
                op = self.stream.trigger()
                op.delete()  # Explicit delete to prevent locking

    def _handle_underflow(self) -> None:
        assert _debug('Player: underflow')
        if self._has_audio_data():
            self._write_to_stream()
        else:
            assert _debug(f'PulseAudioPlayer: Schedule on_eos at index {self._write_index}')
            self._events.append((self._write_index, MediaEvent('on_eos')))

    def _process_events(self, *_) -> None:
        if not self._events:
            assert _debug('PulseAudioPlayer._process_events: No events')
            return

        # Assume this is called after time sync
        timing_info = self.stream.get_timing_info()
        if not timing_info:
            assert _debug('PulseAudioPlayer._process_events: No timing info to process events')
            return

        read_index = timing_info.read_index
        assert _debug(f'PulseAudioPlayer._process_events: Dispatch events at index {read_index}')
        self.dispatch_media_events(read_index)

    def delete(self) -> None:
        assert _debug('Delete PulseAudioPlayer')

        self.stream.pop_handlers()
        driver = self.driver()
        if driver is None:
            assert _debug('PulseAudioDriver has been garbage collected.')
            self.stream = None
            return

        if driver.mainloop is None:
            assert _debug('PulseAudioDriver already deleted. '
                      'PulseAudioPlayer could not clean up properly.')
            return

        with driver.mainloop.lock:
            if self._time_sync_operation is not None:
                self._time_sync_operation.delete()
                self._time_sync_operation = None

            self.stream.delete()
            self.stream = None

    def clear(self) -> None:
        assert _debug('PulseAudioPlayer.clear')
        super(PulseAudioPlayer, self).clear()
        self._clear_write = True
        self._write_index = self._get_read_index()
        self._timestamps = []

        self._read_index_valid = False
        with self.stream.mainloop.lock:
            self.stream.prebuf().wait()

    def play(self) -> None:
        assert _debug('PulseAudioPlayer.play')

        with self.stream.mainloop.lock:
            if self.stream.is_corked():
                self.stream.resume().wait().delete()
                assert _debug('PulseAudioPlayer: Resumed playback')
            if self.stream.underflow:
                self._write_to_stream()
            if not self._has_audio_data():
                self.stream.trigger().wait().delete()
                assert _debug('PulseAudioPlayer: Triggered stream for immediate playback')
            assert not self.stream.is_corked()

        self._playing = True

    def stop(self) -> None:
        assert _debug('PulseAudioPlayer.stop')

        with self.stream.mainloop.lock:
            if not self.stream.is_corked():
                self.stream.pause().wait().delete()

        self._playing = False

    def _get_read_index(self) -> int:
        with self.stream.mainloop.lock:
            self.stream.update_timing_info().wait().delete()
            timing_info = self.stream.get_timing_info()

        read_index = 0 if timing_info is None else timing_info.read_index

        assert _debug('_get_read_index ->', read_index)
        return read_index

    def _get_write_index(self) -> int:
        with self.stream.mainloop.lock:
            timing_info = self.stream.get_timing_info()

        write_index = 0 if timing_info is None else timing_info.write_index

        assert _debug('_get_write_index ->', write_index)
        return write_index

    def _update_and_get_timing_info(self) -> Optional[pa.pa_timing_info]:
        with self.stream.mainloop.lock:
            self.stream.update_timing_info().wait().delete()
            return self.stream.get_timing_info()

    def get_time(self) -> float:
        if not self._read_index_valid:
            assert _debug('get_time <_read_index_valid = False> -> 0')
            return 0

        t_info = self._update_and_get_timing_info()
        read_index = t_info.read_index
        transport_usec = t_info.transport_usec
        sink_usec = t_info.sink_usec

        write_index = 0
        timestamp = 0.0

        try:
            write_index, timestamp = self._timestamps[0]
            write_index, timestamp = self._timestamps[1]
            while read_index >= write_index:
                del self._timestamps[0]
                write_index, timestamp = self._timestamps[1]
        except IndexError:
            pass

        bytes_per_second = self.source.audio_format.bytes_per_second
        dt = (read_index - write_index) / float(bytes_per_second) * 1000000
        # We add 2x the transport time because we didn't take it into account
        # when we wrote the write index the first time. See _write_to_stream
        dt += transport_usec * 2
        dt -= sink_usec
        # We convert back to seconds
        dt /= 1000000
        time = timestamp + dt

        assert _debug('get_time ->', time)
        return time

    def set_volume(self, volume: float) -> None:
        self._volume = volume

        if self.stream:
            driver = self.driver()
            volume *= driver._listener._volume
            with driver.context.mainloop.lock:
                driver.context.set_input_volume(self.stream, volume).wait().delete()

    def set_pitch(self, pitch):
        with self.stream.mainloop.lock:
            sample_rate = self.stream.get_sample_spec().rate
            self.stream.update_sample_rate(int(pitch * sample_rate)).wait().delete()

    def prefill_audio(self):
        self._write_to_stream(nbytes=None)
