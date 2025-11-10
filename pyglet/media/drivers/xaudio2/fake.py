from __future__ import annotations

import ctypes
from ctypes import byref
from decimal import Decimal
from time import perf_counter
import threading

from pyglet.media.drivers.xaudio2 import lib_xaudio2 as lib
from pyglet.libs.win32.com import S_OK


class FakeXAudio2Voice:
    def __init__(
        self,
        driver: FakeXAudio2Driver,
        flags,
        effect_chain,
    ) -> None:
        # Flags XAUDIO2_VOICE_{NOPITCH,NOSRC,USEFILTER} are unused by pyglet and ignored.
        self._driver = driver
        self._volume = 0.0

        self._voice_details_struct = lib.XAUDIO2_VOICE_DETAILS(flags, flags, 0, 0)

    def GetVolume(self, volume_ptr) -> None:
        # Seems to be the only way to write to a byref object
        ctypes.memmove(volume_ptr,
                       ctypes.addressof(ctypes.c_float(self._volume)),
                       ctypes.sizeof(ctypes.c_float))

    def SetVolume(self, volume: float, op_set: int) -> int:
        self._volume = volume
        # No output, volume irrelevant beyond this
        # self._driver.fake_voice_set_volume(self, volume, op_set)
        return S_OK

    def GetVoiceDetails(self, voice_details_ptr) -> None:
        # only the InputChannels member is used.
        ctypes.memmove(voice_details_ptr,
                       ctypes.addressof(self._voice_details_struct),
                       ctypes.sizeof(self._voice_details_struct))

    def SetOutputVoices(self, voice_sends_ptr) -> int:
        raise NotImplementedError()

    def SetEffectChain(self, effect_chain_ptr) -> int:
        raise NotImplementedError()

    def EnableEffect(self, effect: int, op_set: int) -> int:
        raise NotImplementedError()

    def DisableEffect(self, effect: int, op_set: int) -> int:
        raise NotImplementedError()

    def GetEffectState(self, effect: int, is_enabled_ptr) -> None:
        raise NotImplementedError()

    def SetEffectParameters(self, effect: int, params_ptr, params_size: int, op_set: int) -> int:
        raise NotImplementedError()

    def GetEffectParameters(self, effect: int, params_ptr, params_size: int) -> int:
        raise NotImplementedError()

    def SetFilterParameters(self, filter_parameters_ptr, op_set: int) -> None:
        raise NotImplementedError()

    def GetFilterParameters(self, filter_parameters_ptr) -> None:
        raise NotImplementedError()

    def SetOutputFilterParameters(self, dest_voice_ptr, filter_parameters_ptr, op_set: int) -> int:
        raise NotImplementedError()

    def GetOutputFilterParameters(self, dest_voice_ptr, filter_parameters_ptr) -> None:
        raise NotImplementedError()

    def SetChannelVolumes(self, channel_count: int, volumes_ptr, op_set: int) -> int:
        raise NotImplementedError()

    def GetChannelVolumes(self, channel_count: int, volumes_ptr) -> None:
        raise NotImplementedError()

    def SetOutputMatrix(
        self,
        dest_voice_ptr,
        source_channel_count: int,
        dest_channel_count: int,
        mat_ptr,
        op_set: int,
    ) -> int:
        # GetOutputMatrix is not used, and this matrix only controls volume, which is unneeded
        # due to the lack of playback.
        return S_OK

    def GetOutputMatrix(self, dest_voice_ptr, source_channel_count: int, dest_channel_count: int, mat_ptr) -> int:
        raise NotImplementedError()

    def DestroyVoice(self) -> None:
        # Ignored, pyglet only destroys voices at interpreter exit, at which point
        # controlled destruction is pointless.
        # If this were to be implemented, an additional lock would need to be created
        # that protects the fake driver's `_voices` array.
        pass


class FakeXAudio2Buffer:
    __slots__ = ("length", "pointer", "absolute_start", "active")

    def __init__(self, length, pointer, absolute_start) -> None:
        self.length = length
        self.pointer = pointer
        self.absolute_start = absolute_start
        self.active = False


class FakeXAudio2SourceVoice(FakeXAudio2Voice):
    """
    Mimic the Windows SourceVoice interface to communicate with a fake driver
    in case XAudio2 drops out due to device changes
    """

    # NOTE: The terms "sample" and "frame" are used interchangeably in many places,
    # but not all of them.
    # XAudio2 unfortunately refers to frames as samples.

    def __init__(
        self,
        driver: FakeXAudio2Driver,
        initial_audio_data,
        initial_frames_played: int,
        absolute_submitted_frame_count: int,
        flags,
        wave_format_ptr,
        effect_chain,
        max_frequency_ratio,
        callback,
        buffer_lock: threading.Lock,
        send_list,
    ) -> None:
        super().__init__(driver, flags, effect_chain)
        # Flags XAUDIO2_VOICE_{NOPITCH,NOSRC,USEFILTER} are unused by pyglet and ignored.

        self._active = False

        # If set to a value >= 0, flush all buffers above this index on next processing pass.
        self._to_flush = -1

        self._absolute_submitted_frame_count = absolute_submitted_frame_count

        self._samples_played = 0
        self._frequency_ratio = 1.0
        self._max_frequency_ratio = max_frequency_ratio
        self._voice_state_struct = lib.XAUDIO2_VOICE_STATE()

        self._wave_format = lib.WAVEFORMATEX()
        ctypes.memmove(byref(self._wave_format), wave_format_ptr, ctypes.sizeof(self._wave_format))
        self._bytes_per_sample = self._wave_format.wBitsPerSample // 8
        self._bytes_per_frame = self._bytes_per_sample * self._wave_format.nChannels

        self._voice_details_struct.InputChannels = 0
        self._voice_details_struct.SampleRate = self._wave_format.nSamplesPerSec

        self._callback = callback
        self._send_list = send_list

        self._buffer_lock = buffer_lock

        self.buffers: list[FakeXAudio2Buffer] = []
        for absolute_start, i in initial_audio_data:
            assert i.length % self._bytes_per_frame == 0
            self.buffers.append(FakeXAudio2Buffer(i.length // self._bytes_per_frame, i.pointer, absolute_start))

        # Measured in frames
        self._current_buffer_offset = initial_frames_played - self.buffers[0].absolute_start if self.buffers else 0
        self._fractional_offset = Decimal(0.0)

        assert self._current_buffer_offset >= 0
        if self.buffers:
            assert self._current_buffer_offset < self.buffers[0].length

        self._frames_consumed_per_second = Decimal(0)
        self._update_frames_consumed_per_second()

    def Start(self, flags: int, op_set: int) -> int:
        assert flags == 0
        if op_set == 0:
            self.start_impl(flags)
        else:
            self._driver.fake_voice_start(self, op_set, flags)
        return S_OK

    def Stop(self, flags: int, op_set: int) -> int:
        if op_set == 0:
            self.stop_impl(flags)
        else:
            self._driver.fake_voice_stop(self, op_set, flags)
        return S_OK

    def SubmitSourceBuffer(self, buf_ptr, wma_buf_ptr) -> None:
        # wma_buf_ptr is unused and ignored
        buf = lib.XAUDIO2_BUFFER()
        ctypes.memmove(ctypes.addressof(buf), buf_ptr, ctypes.sizeof(lib.XAUDIO2_BUFFER))
        assert buf.AudioBytes % self._bytes_per_frame == 0
        self.buffers.append(FakeXAudio2Buffer(
            buf.AudioBytes // self._bytes_per_frame,
            buf.pAudioData,
            self._absolute_submitted_frame_count))
        self._absolute_submitted_frame_count += buf.AudioBytes // self._bytes_per_sample

    def GetState(self, voice_state_ptr, flags: int) -> None:
        # Ignore the NO_SAMPLES_PLAYED flag

        # Buffer context never supplied by pyglet, ignore
        self._voice_state_struct.pCurrentBufferContext = None
        self._voice_state_struct.BuffersQueued = len(self.buffers)
        self._voice_state_struct.SamplesPlayed = self._samples_played

        # print(f"fake voice BQ: {self._voice_state_struct.BuffersQueued}, SP: {self._voice_state_struct.SamplesPlayed}")

        ctypes.memmove(voice_state_ptr,
                       ctypes.addressof(self._voice_state_struct),
                       ctypes.sizeof(self._voice_state_struct))

    def FlushSourceBuffers(self) -> int:
        with self._buffer_lock:
            if self._active:
                self._to_flush = 1
            else:
                self._to_flush = 0
        return S_OK

    def GetFrequencyRatio(self, fr_ptr) -> None:
        ctypes.memmove(fr_ptr,
                       ctypes.addressof(ctypes.c_float(self._frequency_ratio)),
                       ctypes.sizeof(ctypes.c_float))

    def SetFrequencyRatio(self, frequency_ratio: float, op_set: int) -> None:
        if op_set == 0:
            self.set_frequency_ratio_impl(frequency_ratio)
        else:
            self._driver.fake_voice_set_frequency_ratio(self, frequency_ratio, op_set)

    def SetSourceSampleRate(self, sr: int) -> int:
        # assert self._voice_details_struct.CreationFlags & (lib.XAUDIO2_VOICE_NOPITCH | lib.XAUDIO2_VOICE_NOSRC) == 0
        assert 1000 <= sr <= 200000
        assert not self.buffers

        self._voice_details_struct.SampleRate = sr
        self._update_frames_consumed_per_second()

        return S_OK

    def ExitLoop(self, op_set: int) -> int:
        raise NotImplementedError()

    def Discontinuity(self) -> int:
        raise NotImplementedError()

    def flush_impl(self) -> None:
        """Perform buffer flushing. May only be called from the FakeDriver run thread."""
        with self._buffer_lock:
            assert self._to_flush >= 0

            while len(self.buffers) > self._to_flush:
                self.buffers.pop(self._to_flush)
                self._buffer_lock.release()
                try:
                    # NOTE: The userdata pointer is not used by pyglet, so we ignore it.
                    self._callback.OnBufferEnd(None)
                finally:
                    self._buffer_lock.acquire()
            self._to_flush = -1

    def start_impl(self, flags: int) -> None:
        self._active = True

    def stop_impl(self, flags: int) -> None:
        self._active = False

    def set_frequency_ratio_impl(self, frequency_ratio: float) -> None:
        if frequency_ratio < 0.0005:
            frequency_ratio = 0.0005
        if frequency_ratio > self._max_frequency_ratio:
            frequency_ratio = self._max_frequency_ratio
        self._frequency_ratio = frequency_ratio
        self._update_frames_consumed_per_second()

    def _update_frames_consumed_per_second(self) -> None:
        self._frames_consumed_per_second = Decimal(self._voice_details_struct.SampleRate * self._frequency_ratio)


class _OperationSet:
    __slots__ = ("change_play_state", "change_freq_ratio")

    def __init__(self) -> None:
        self.change_play_state = {}
        self.change_freq_ratio = {}


class FakeXAudio2Driver:
    # XAudio2 crashes pretty ungracefully when audio devices are unplugged. For consistency with
    # OpenAL and PulseAudio, a good-enough-effort fake driver is supplanted which causes audio
    # players to continue delivering data into a facade. The driver fakes playback as well as
    # possible and also supports the basic operation set functionality utilized by pyglet's
    # internals.
    # Perfecting this is infeasible, as it'd necessitate an implementation to such an extent that it
    # can keep track of a complete voice graph / playing / effect state to load as soon as the
    # actual driver returns.
    # This will fail if an application hacks past pyglet and uses XAudio2-specifics such as effect
    # chains, looping buffers or callbacks such as OnVoiceProcessingPassStart, for example.

    PROCESSING_INTERVAL = 0.01

    def __init__(self, driver) -> None:
        self._voices = []
        self._driver = driver
        self._thread = None
        self._stop_event = None
        self._last_processing_step_time = 0.0
        self._operation_sets = {}
        self._committed_operation_sets = []
        self._operation_lock = threading.Lock()

    def CreateSourceVoice(self, *_, **__):
        raise NotImplementedError("Create sources on the fake driver via create_source_voice")

    def CommitChanges(self, op_set_id: int) -> None:
        with self._operation_lock:
            if (set_ := self._operation_sets.pop(op_set_id, None)) is not None:
                self._committed_operation_sets.append(set_)

    def _create_operation_set(self, op_set_id: int) -> _OperationSet:
        if op_set_id not in self._operation_sets:
            self._operation_sets[op_set_id] = _OperationSet()
        return self._operation_sets[op_set_id]

    def fake_voice_set_frequency_ratio(self, voice: FakeXAudio2Voice, frequency_ratio: float, op_set: int) -> None:
        with self._operation_lock:
            self._create_operation_set(op_set).change_freq_ratio[voice] = frequency_ratio

    def fake_voice_start(self, voice: FakeXAudio2Voice, op_set: int, flags: int) -> None:
        with self._operation_lock:
            self._create_operation_set(op_set).change_play_state[voice] = (True, flags)

    def fake_voice_stop(self, voice: FakeXAudio2Voice, op_set: int, flags: int) -> None:
        with self._operation_lock:
            self._create_operation_set(op_set).change_play_state[voice] = (False, flags)

    # NOTE: This method passes some higher python primitives and information. This removes
    # the fake driver's need of from having to handle loop info to pick up playback in the
    # middle of a buffer, at the cost of a deviating voice creation method.
    # TODO: It also sucks, simplify in the future.
    def create_source_voice(
        self,
        initial_audio_data,
        initial_samples_played,
        absolute_submitted_frame_count,
        buffer_lock,
        wave_format_ptr,
        flags,
        max_frequency_ratio,
        callback,
        send_list,
        effect_chain,
    ) -> FakeXAudio2SourceVoice:
        v = FakeXAudio2SourceVoice(self,
                                   initial_audio_data,
                                   initial_samples_played,
                                   absolute_submitted_frame_count,
                                   flags,
                                   wave_format_ptr,
                                   effect_chain,
                                   max_frequency_ratio,
                                   callback,
                                   buffer_lock,
                                   send_list)

        self._voices.append(v)
        return v

    def start(self) -> None:
        self._last_processing_step_time = self._driver._time_of_death
        self._thread = threading.Thread(target=self.run, daemon=True)

        self._voices = [v._voice for v in self._driver._iter_voices()]

        self._stop_event = threading.Event()
        self._thread.start()

    def stop(self) -> None:
        if self._thread is not None:
            self._stop_event.set()
            self._thread.join()
            self._thread = None

    def run(self) -> None:
        # There is no more audio-driver-based time source, fake it using perf_counter.

        # This method's implementation was partially taken from Wine's FAudio source code.

        next_wake_time = perf_counter()

        while True:
            with self._operation_lock:
                # NOTE: OperationSet 0 is never created, its effects always apply instantly.
                # If that wasn't the case, it'd probably need to be added into
                # `_committed_operation_sets` as well here.
                for set_ in self._committed_operation_sets:
                    for v, (play_state, flags) in set_.change_play_state.items():
                        if play_state:
                            v.start_impl(flags)
                        else:
                            v.stop_impl(flags)
                    for v, freq_ratio in set_.change_freq_ratio.items():
                        v.set_frequency_ratio_impl(freq_ratio)

            tstamp = perf_counter()
            tdiff = Decimal(tstamp - self._last_processing_step_time)
            self._last_processing_step_time = tstamp

            self._driver._engine_callback.OnProcessingPassStart()

            # XAudio2 operates on a 10ms step and tends to report samples_played in increments
            # of those, so try and mimick that.
            # Also note that "samples" are actually frames in most places.

            # Iterate like this as self._voices may be modified (but only by append) during iteration.
            # Since it never shrinks and voices are never reordered, this should be fine.
            vidx = 0
            while vidx < len(self._voices):
                v: FakeXAudio2SourceVoice = self._voices[vidx]
                vidx += 1

                if v._to_flush >= 0:
                    v.flush_impl()

                if not v._active:
                    continue

                # Hopefully decimal saves us from floating point error well enough
                advance = (v._frames_consumed_per_second * tdiff) + v._fractional_offset
                advance_int = int(advance)
                v._fractional_offset = advance - advance_int

                # Ignore and do not dispatch OnVoiceProcessingPassStart as pyglet doesn't make use of it.
                # Implementing the "minimum amounts of immediate bytes required to avoid starvation"
                # calculation is pointless too as pyglet always buffers a few hundred ms of data.

                drained = 0
                while drained < advance_int and v.buffers:
                    to_drain = advance_int - drained
                    if not v.buffers[0].active:
                        v.buffers[0].active = True
                        # Ignore unused OnBufferStart callback
                        # v._callback.OnBufferStart()

                    x = min(v.buffers[0].length - v._current_buffer_offset, to_drain)
                    drained += x

                    v._current_buffer_offset += x
                    v._samples_played += x

                    if x < to_drain:
                        # Buffer exhausted
                        # Lock is probably not needed here as `pop` will be atomic.
                        v.buffers.pop(0)
                        v._current_buffer_offset = 0

                        # Ignore OnBufferEnd userdata pointer.
                        # Ignore OnStreamEnd callback.
                        v._callback.OnBufferEnd(None)

                # Ignore OnVoiceProcessingPassEnd

            self._driver._engine_callback.OnProcessingPassEnd()

            next_wake_time += self.PROCESSING_INTERVAL
            if self._stop_event.wait(timeout=max(next_wake_time - perf_counter(), 0.0)):
                break
