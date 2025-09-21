from __future__ import annotations

import decimal
from math import ceil
import threading
from time import perf_counter, sleep
import weakref
from collections import defaultdict, deque, namedtuple
from ctypes import POINTER, byref, c_char, c_float, cast, pointer
from ctypes.wintypes import DWORD, FLOAT

import pyglet
from pyglet.libs.win32.types import *
from pyglet.libs.win32.com import S_OK
from pyglet.util import debug_print
from pyglet.media.codecs.base import AudioFormat
from pyglet.media.devices import get_audio_device_manager
from pyglet.media.devices.base import DeviceFlow
from pyglet.util import debug_print
from pyglet.media.devices import get_audio_device_manager
from . import lib_xaudio2 as lib

_debug = debug_print('debug_media')


def create_xa2_buffer(audio_data):
    """Creates a XAUDIO2_BUFFER to be used with a source voice.
        Audio data cannot be purged until the source voice has played it; doing so will cause glitches."""
    buff = lib.XAUDIO2_BUFFER()
    buff.AudioBytes = audio_data.length
    buff.pAudioData = ctypes.cast(audio_data.pointer, ctypes.POINTER(ctypes.c_char))
    return buff


def create_xa2_waveformat(audio_format):
    wfx = lib.WAVEFORMATEX()
    wfx.wFormatTag = lib.WAVE_FORMAT_PCM
    wfx.nChannels = audio_format.channels
    wfx.nSamplesPerSec = audio_format.sample_rate
    wfx.wBitsPerSample = audio_format.sample_size
    wfx.nBlockAlign = wfx.wBitsPerSample * wfx.nChannels // 8
    wfx.nAvgBytesPerSec = wfx.nSamplesPerSec * wfx.nBlockAlign
    return wfx


class FakeXAudio2Voice:
    def __init__(
        self,
        driver: _FakeDriver,
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
        # TODO: This method takes over the processing lock
        pass


class FakeXAudio2MasteringVoice(FakeXAudio2Voice):
    """
    Mimic the XAudio2 MasteringVoice interface.
    """
    def __init__(
        self,
        driver: _FakeDriver,
        flags,
        effect_chain,
        input_channels: int,
        input_sample_rate: int,
        device_id,
        stream_category: int,
    ) -> None:
        super().__init__(driver, flags, effect_chain)

        # Since no audio is ever played, these values work fine to fake a setup.
        # TODO: Take the original master voice's details.
        # If there is none, bad luck. I guess go with 44100?
        self._voice_details_struct.InputChannels = 1
        self._voice_details_struct.SampleRate = 44100

    def GetChannelMask(self, channel_mask_ptr) -> int:
        # TODO
        raise NotImplementedError()


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
        driver: _FakeDriver,
        initial_audio_data,
        initial_frames_played: int,
        absolute_submitted_frame_count: int,
        active: bool,
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

        self._active = active

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
        self._fractional_offset = decimal.Decimal(0.0)

        assert self._current_buffer_offset >= 0
        if self.buffers:
            assert self._current_buffer_offset < self.buffers[0].length

        self._frames_consumed_per_second = decimal.Decimal(0)
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
        self._frames_consumed_per_second = decimal.Decimal(
            self._voice_details_struct.SampleRate * self._frequency_ratio)


class _OperationSet:
    __slots__ = ("change_play_state", "change_freq_ratio")

    def __init__(self) -> None:
        self.change_play_state = {}
        self.change_freq_ratio = {}


class _FakeDriver:
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
        raise NotImplementedError()

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

    # NOTE: This method passes some higher python primitives and information in which removes
    # the fake driver's need of from having to handle loop info to pick up playback in the
    # middle of a buffer.
    # TODO: It also sucks, simplify in the future.
    def create_source_voice(
        self,
        initial_audio_data,
        initial_samples_played,
        absolute_submitted_frame_count,
        active,
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
                                   active,
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
                # `_committed_operation_sets`` as well here.
                for set_ in self._committed_operation_sets:
                    for v, (play_state, flags) in set_.change_play_state.items():
                        if play_state:
                            v.start_impl(flags)
                        else:
                            v.stop_impl(flags)
                    for v, freq_ratio in set_.change_freq_ratio.items():
                        v.set_frequency_ratio_impl(freq_ratio)

            tstamp = perf_counter()
            tdiff = decimal.Decimal(tstamp - self._last_processing_step_time)
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
                        v.buffers.pop(0)
                        v._current_buffer_offset = 0

                        # TODO locking?
                        # Ignore OnBufferEnd userdata pointer.
                        # Ignore OnStreamEnd.
                        v._callback.OnBufferEnd(None)

                # Ignore OnVoiceProcessingPassEnd

            self._driver._engine_callback.OnProcessingPassEnd()

            next_wake_time += self.PROCESSING_INTERVAL
            if self._stop_event.wait(timeout=max(next_wake_time - perf_counter(), 0.0)):
                break


class XA2EngineCallback(com.COMObject):
    _interfaces_ = [lib.IXAudio2EngineCallback]

    def __init__(self, driver):
        super().__init__()
        self._driver = driver

    def OnProcessingPassStart(self):
        self._driver._last_processing_step_time = perf_counter()
        self._driver.lock.acquire()

    def OnProcessingPassEnd(self):
        self._driver.lock.release()

    def OnCriticalError(self, hresult):
        assert _debug(f"XAudio2EngineCallback.OnCriticalError: {hresult}")


class XAudio2VoiceCallback(com.COMObject):
    """Callback class used to trigger when buffers or streams end.
           WARNING: Whenever a callback is running, XAudio2 cannot generate audio.
           Make sure these functions run as fast as possible and do not block/delay more than a few milliseconds.
           MS Recommendation:
           At a minimum, callback functions must not do the following:
                - Access the hard disk or other permanent storage
                - Make expensive or blocking API calls
                - Synchronize with other parts of client code
                - Require significant CPU usage
    """
    _interfaces_ = [lib.IXAudio2VoiceCallback]

    def __init__(self):
        super().__init__()
        self.on_buffer_end = None

    def OnBufferEnd(self, pBufferContext):
        self.on_buffer_end(pBufferContext)

    def OnVoiceError(self, pBufferContext, hresult):
        raise Exception(f"Error occurred during audio playback: {hresult}")


class XAudio2Driver:
    # Specifies if positional audio should be used. Can be enabled later, but not disabled.
    allow_3d = True

    # Which processor to use. (#1 by default)
    processor = lib.XAUDIO2_DEFAULT_PROCESSOR

    # Which stream classification Windows uses on this driver.
    category = lib.AudioCategory_GameEffects

    # If the driver errors or disappears, it will attempt to restart the engine.
    restart_on_error = True

    # Max Frequency a voice can have. Setting this higher/lower will increase/decrease memory allocation.
    max_frequency_ratio = 2.0

    def __init__(self, pyglet_driver) -> None:
        """Creates an XAudio2 master voice and sets up 3D audio if specified. This attaches to the default audio
        device and will create a virtual audio endpoint that changes with the system. It will not recover if a
        critical error is encountered such as no more audio devices are present.
        """
        assert _debug('Constructing XAudio2Driver')
        self._pyglet_driver = pyglet_driver
        self._fake_driver = _FakeDriver(self)

        self._listener = None
        self._xaudio2 = None
        self._dead = False
        self._time_of_death = 0.0
        self._last_processing_step_time = 0.0

        # A lock that will prevent XAudio2 from running any callbacks (processing audio at all)
        # while it is held. Must be acquired by audio players in certain situations in order to
        # ensure that the following, very unlikely, sequence of events does not happen:
        # - an on_buffer_end callback is made
        # - python creates a dummy thread to run its code
        # - very early on, before it could acquire any protective locks, the thread is suspended
        #   and the main thread runs
        # - the main thread runs a critical operation on the player such as `delete` to completion
        # - the callback is resumed and breaks as the audio player is deleted.
        self.lock = threading.Lock()
        self._engine_callback = XA2EngineCallback(self)

        self._voice_pool_lock = threading.Lock()

        self._voices_emitting = []  # Contains all in-use source voices with an emitter.
        self._voices_in_use = {}  # All voices currently in use, mapped to their audio player.
        self._voice_pool = defaultdict(list)  # Maps voice keys to lists of voices ready to use.
        self._voices_resetting = {}  # All resetting voices, mapped to their resetter.

        self._x3d_handle = None
        self._dsp_settings = None

        try:
            self._create_xa2()
        except OSError:
            self._dead = True
            self._setup_fake_driver()

        if self.restart_on_error:
            audio_devices = get_audio_device_manager()
            if audio_devices:
                assert _debug('Audio device instance found.')
                audio_devices.push_handlers(self)
                pyglet.clock.schedule_interval_soft(self._check_state, 0.5)

    def _iter_voices(self):
        yield from self._voices_resetting.keys()
        for pool in self._voice_pool.values():
            yield from pool
        yield from self._voices_in_use

    def on_default_changed(self, device, flow: DeviceFlow):
        if flow == DeviceFlow.OUTPUT:
            """Callback derived from the Audio Devices to help us determine when the system no longer has output."""
            if device is None:
                assert _debug('Error: Default audio device was removed or went missing.')
                self._time_of_death = perf_counter()
                self._dead = True
            else:
                if self._dead:
                    assert _debug('Warning: Default audio device added after going missing.')
                    self._dead = False

    def _check_state(self, dt):
        """Hack/workaround, you cannot shutdown/create XA2 within a COM callback, set a schedule to check state."""
        if self._dead is True:
            if self._xaudio2:
                assert _debug("XAudio2Driver._check_state: shutting down")

                self._setup_fake_driver()
        else:
            if not self._xaudio2:
                assert _debug("XAudio2Driver._check_state: recreating and resetting")
                self._create_xa2()
                self._recreate_voices()

    def _setup_fake_driver(self):
        # Exchange all existing voices for fake voices.
        _debug(f"Installing fake voices {self._voice_pool} {self._voices_in_use}")

        with self._voice_pool_lock:
            self._reset_resetting_voices()

            for v in self._iter_voices():
                v.create_fake_voice(self)

        self._delete_driver(destroy_voices=False)

        self._fake_driver.start()

    def _reset_resetting_voices(self):
        for v in self._voices_resetting:
            self._return_reset_voice(v)

    def _recreate_voices(self):
        _debug("Stopping fake driver")
        # NOTE: Will wait on a thread. Probably not too serious.
        self._fake_driver.stop()

        _debug(f"Recreating real voices {self._voice_pool} {self._voices_in_use}")
        with self._voice_pool_lock:
            # Flushing voices won't have OnBufferEnd callbacks called anymore.
            # Flush them instantly here to update the voice gates, plugging in new
            # voices should be good then.
            self._reset_resetting_voices()

            for (channel_count, sample_size), list_ in self._voice_pool.items():
                for voice in list_:
                    voice.phantom_samples_played = voice.samples_played
                    voice._voice = self._create_new_real_voice(AudioFormat(channel_count, sample_size, 44100), voice._callback)

            # Playing voices need to be started now
            for voice, player in self._voices_in_use.items():
                # TODO: remove passing in of audio format
                voice.create_real_voice_and_restart(player.source.audio_format, self)

    def _create_xa2(self, device_id=None):
        self._xaudio2 = lib.IXAudio2()

        try:
            lib.XAudio2Create(ctypes.byref(self._xaudio2), 0, self.processor)
        except OSError:
            self._xaudio2 = None
            raise

        if _debug:
            # Debug messages are found in Windows Event Viewer, you must enable event logging:
            # Applications and Services -> Microsoft -> Windows -> Xaudio2 -> Debug Logging.
            # Right click -> Enable Logs
            debug = lib.XAUDIO2_DEBUG_CONFIGURATION()
            debug.LogThreadID = True
            debug.TraceMask = lib.XAUDIO2_LOG_ERRORS | lib.XAUDIO2_LOG_WARNINGS
            debug.BreakMask = lib.XAUDIO2_LOG_WARNINGS

            self._xaudio2.SetDebugConfiguration(ctypes.byref(debug), None)

        self._xaudio2.RegisterForCallbacks(self._engine_callback)

        self._mvoice_details = lib.XAUDIO2_VOICE_DETAILS()
        self._master_voice = lib.IXAudio2MasteringVoice()

        try:
            self._xaudio2.CreateMasteringVoice(byref(self._master_voice),
                                               lib.XAUDIO2_DEFAULT_CHANNELS,
                                               lib.XAUDIO2_DEFAULT_SAMPLERATE,
                                               0, device_id, None, self.category)
        except OSError:
            self._xaudio2.Release()
            self._xaudio2 = None
            raise

        self._master_voice.GetVoiceDetails(byref(self._mvoice_details))

        self._listener = XAudio2Listener(self)
        self._pyglet_driver._listener.connect(self, self._listener)

        if self.allow_3d:
            self.enable_3d()

    def enable_3d(self):
        """Initializes the prerequisites for 3D positional audio and initializes with default DSP settings."""
        channel_mask = DWORD()
        self._master_voice.GetChannelMask(byref(channel_mask))

        self._x3d_handle = lib.X3DAUDIO_HANDLE()
        lib.X3DAudioInitialize(channel_mask.value, lib.X3DAUDIO_SPEED_OF_SOUND, self._x3d_handle)

        matrix = (FLOAT * self._mvoice_details.InputChannels)()
        self._dsp_settings = lib.X3DAUDIO_DSP_SETTINGS()
        self._dsp_settings.SrcChannelCount = 1
        self._dsp_settings.DstChannelCount = self._mvoice_details.InputChannels
        self._dsp_settings.pMatrixCoefficients = matrix

        pyglet.clock.schedule_interval_soft(self._calculate_3d_sources, 1 / 15.0)

    def _destroy_voices(self):
        """Destroy and clear all voice pools."""
        for list_ in self._voice_pool.values():
            for voice in list_:
                voice.destroy()
            list_.clear()

        for voice, resetter in self._voices_resetting.items():
            voice.destroy()
            resetter.destroy()
        self._voices_resetting.clear()

        self._voices_emitting.clear()
        for voice in self._voices_in_use.keys():
            voice.destroy()
        self._voices_in_use.clear()

    def _delete_driver(self, destroy_voices=True):
        if self._xaudio2 is None:
            return

        assert _debug("XAudio2Driver: Deleting")

        self._pyglet_driver._listener.disconnect()
        self._listener = None

        # Stop 3d
        if self.allow_3d:
            pyglet.clock.unschedule(self._calculate_3d_sources)
            self._x3d_handle = None
            self._dsp_settings = None

        # Destroy all pooled voices as master will change.
        if destroy_voices:
            self._destroy_voices()

        self._xaudio2.UnregisterForCallbacks(self._engine_callback)
        self._xaudio2.StopEngine()
        self._xaudio2.Release()
        self._xaudio2 = None

    def _calculate_3d_sources(self, dt):
        """We calculate the 3d emitters and sources every 15 fps, committing everything after deferring all changes."""
        for source_voice in self._voices_emitting:
            self._apply3d(source_voice, 1)

        self._xaudio2.CommitChanges(1)

    def apply3d(self, source_voice):
        """Apply and immediately commit positional audio effects for the given voice."""
        if self._x3d_handle is not None:
            self._apply3d(source_voice, 2)
            self._xaudio2.CommitChanges(2)

    def _apply3d(self, source_voice, commit):
        """Calculates and sets output matrix and frequency ratio on the voice based on the listener and the voice's
           emitter. Commit determines the operation set, whether the settings are applied immediately (0) or to
           be committed together at a later time.
        """
        lib.X3DAudioCalculate(
            self._x3d_handle,
            self._listener.listener,
            source_voice._emitter,
            lib.default_dsp_calculation,
            self._dsp_settings,
        )
        source_voice._voice.SetOutputMatrix(self._master_voice,
                                            1,
                                            self._mvoice_details.InputChannels,
                                            self._dsp_settings.pMatrixCoefficients,
                                            commit)

        source_voice._voice.SetFrequencyRatio(self._dsp_settings.DopplerFactor, commit)

    def delete(self):
        pyglet.clock.unschedule(self._check_state)
        self._delete_driver()
        self._fake_driver.stop()

    def get_performance(self):
        """Retrieve some basic XAudio2 performance data such as memory usage and source counts."""
        pf = lib.XAUDIO2_PERFORMANCE_DATA()
        self._xaudio2.GetPerformanceData(ctypes.byref(pf))
        return pf

    def _reset_voice_on_buffer_end(self, voice) -> None:
        if voice.buffers_queued == 0:
            # Due to some asynchronity when calling Stop(0, 0), we can only be reasonably sure
            # a voice is stopped and ready for getting repooled right here.
            self._return_reset_voice(voice)

    def _return_reset_voice(self, voice) -> None:
        voice.audio_data_in_use.clear()

        samples_played = voice.samples_played
        voice._callback.on_buffer_end = None
        voice.samples_played_at_last_recycle = samples_played
        voice._absolute_submitted_frame_count = samples_played

        voice_key = (voice.channel_count, voice.sample_size)

        # Another thread may theoretically interfere right here.
        # The worst outcome of that would be the voice getting overlooked during a driver
        # swapout.
        # However, this function is called either while _voice_pool_lock is held,
        # or from within an XAudio2 callback, where the global lock is held, where such
        # a swapout won't happen. It should be safe.
        self._voices_resetting.pop(voice)
        self._voice_pool[voice_key].append(voice)
        assert _debug(f"XA2AudioDriver: {voice} back in pool")

    def return_voice(self, voice: "XA2SourceVoice"):
        """Reset a voice and eventually return it to the pool. The voice must be stopped."""
        with self._voice_pool_lock:
            if voice.is_emitter:
                self._voices_emitting.remove(voice)
            self._voices_in_use.pop(voice)

            assert _debug(f"XA2AudioDriver: Resetting {voice}...")

            self._voices_resetting[voice] = None
            if voice.buffers_queued != 0:
                # If the audio thread ran right here and we would now be at zero buffers,
                # the callback would never be invoked.
                # For this reason, we explicitly check again below.
                voice._callback.on_buffer_end = lambda *_, v=voice: self._reset_voice_on_buffer_end(v)
                voice.flush()
                if voice.buffers_queued == 0:
                    # We have definitely been interrupted. That's not good.
                    # A: Maybe the python callback has already run.
                    # B: Maybe the python callback is running RIGHT now. (very unlikely but not impossible.
                    #    I believe callbacks from native code can be paused in favor of other threads)
                    # C: Maybe the python callback will run soon.
                    # D: Maybe the python callback will never run.
                    # As D is a possibility, grab the engine lock to ensure it's not B and return the voice
                    # if it wasn't already.
                    with self.lock:
                        if voice in self._voices_resetting:
                            self._return_reset_voice(voice)
            else:
                self._return_reset_voice(voice)

    def get_source_voice(self, audio_format, player):
        """Get a source voice from the pool. Source voice creation can be slow to create/destroy.
        So pooling is recommended. We pool based on audio channels and sample size.
        A source voice handles all of the audio playing and state for a single source."""

        voice = self._get_or_create_source_voice(audio_format)

        voice.acquired(player.on_buffer_end, audio_format.sample_rate)
        if voice.is_emitter:
            self._voices_emitting.append(voice)
        self._voices_in_use[voice] = player

        return voice

    def _get_or_create_source_voice(self, audio_format):
        voice_key = (audio_format.channels, audio_format.sample_size)

        with self._voice_pool_lock:
            if not self._voice_pool[voice_key]:
                voice = self._create_new_voice(audio_format)
                # Create a 2nd one for good measure, multiple players might be needing it soon,
                # and a clear command will probably complete more quickly when swapping out for a
                # pooled voice
                self._voice_pool[voice_key].append(self._create_new_voice(audio_format))
            else:
                voice = self._voice_pool[voice_key].pop()

        assert voice.buffers_queued == 0

        return voice

    def _create_new_voice(self, audio_format):
        """Has the driver create a new source voice for the given audio format."""
        callback = XAudio2VoiceCallback()
        buffer_lock = threading.Lock()

        if self._xaudio2 is None:
            voice = self._create_new_fake_voice(audio_format, callback, buffer_lock)
        else:
            voice = self._create_new_real_voice(audio_format, callback)

        return XA2SourceVoice(voice, callback, buffer_lock, audio_format)

    def _create_new_fake_voice(
        self,
        audio_format,
        callback,
        buffer_lock,
        initial_audio_data = (),
        initial_samples_played = 0,
        absolute_submitted_frame_count = 0,
        active = False,
    ):
        wfx = create_xa2_waveformat(audio_format)
        return self._fake_driver.create_source_voice(
            initial_audio_data,
            initial_samples_played,
            absolute_submitted_frame_count,
            active,
            buffer_lock,
            byref(wfx),
            0,
            self.max_frequency_ratio,
            callback,
            None,
            None,
        )

    def _create_new_real_voice(self, audio_format, callback):
        voice = lib.IXAudio2SourceVoice()
        wfx = create_xa2_waveformat(audio_format)
        self._xaudio2.CreateSourceVoice(byref(voice),
                                        byref(wfx),
                                        0,
                                        self.max_frequency_ratio,
                                        callback,
                                        None,
                                        None)
        return voice


class XA2SourceVoice:
    def __init__(self, voice, callback, buffer_lock, audio_format: "AudioFormat") -> None:
        self._voice_state = lib.XAUDIO2_VOICE_STATE()  # Will be reused constantly.
        self._voice = voice
        self._callback = callback

        # A lock to prevent interference from PWT or user threads when driver implementations
        # are swapped out.
        # This lock is shared by a fake voice as well.
        self._buffer_lock = buffer_lock

        # Another lock to protect against some data races
        self._lock = threading.Lock()

        self._playing = False

        # Store the audio data currently in use.
        # Each entry is a two-size tuple with a buffer's first frame in respect to this voice's
        # total lifetime and the AudioData containing the buffer's data.
        self.audio_data_in_use = deque()

        assert audio_format.sample_size % 8 == 0

        self.channel_count = audio_format.channels
        self.sample_size = audio_format.sample_size
        self._bytes_per_frame = (audio_format.sample_size // 8) * audio_format.channels

        # May be modified as the voice is acquired by different AudioPlayers.
        self._sample_rate = audio_format.sample_rate

        # TODO: Since we now are lying about samples_played (not returning the true underlying
        # value), consider moving the samples_played_at_last_recycle business into it too.

        # Samples played by underlying voices that have been replaced since but still
        # need to be cheated into the `samples_played` calculation.
        self.phantom_samples_played = 0

        # How many samples the voice had played when it was most recently readded into the
        # pool of available voices.
        self.samples_played_at_last_recycle = 0

        # How many samples have been buffered on this voice throughout its lifetime.
        # This number is used to give each submitted buffer its absolute start position with
        # respect to `samples_played`.
        # Note that if this voice is reset, this number is reduced to `samples_played`.
        self._absolute_submitted_frame_count = 0

        # If it's a mono source, then we can make it an emitter.
        # In the future, non-mono sources can be supported as well.
        if audio_format.channels == 1:
            self._emitter = lib.X3DAUDIO_EMITTER()
            self._emitter.ChannelCount = audio_format.channels
            self._emitter.CurveDistanceScaler = 1.0

            # Commented are already set by the Player class.
            # Leaving for visibility on default values
            cone = lib.X3DAUDIO_CONE()
            # cone.InnerAngle = math.radians(360)
            # cone.OuterAngle = math.radians(360)
            cone.InnerVolume = 1.0
            # cone.OuterVolume = 1.0

            self._emitter.pCone = pointer(cone)
            self._emitter.pVolumeCurve = None
        else:
            self._emitter = None

    def destroy(self):
        """Completely destroy the voice."""
        self._emitter = None

        if self._voice is not None:
            self._voice.DestroyVoice()
            self._voice = None

        self._callback = None

    def acquired(self, on_buffer_end_cb, sample_rate):
        """A voice has been acquired. Set the callback as well as its new sample
        rate.
        """
        self._callback.on_buffer_end = on_buffer_end_cb
        self._voice.SetSourceSampleRate(sample_rate)
        self._sample_rate = sample_rate

    @property
    def buffers_queued(self):
        """Get the amount of buffers in the current voice. Adding flag for no samples played is 3x faster."""
        self._voice.GetState(byref(self._voice_state), lib.XAUDIO2_VOICE_NOSAMPLESPLAYED)
        # _debug(f"XA2SourceVoice.buffers_queued: {self._voice_state.BuffersQueued}")
        return self._voice_state.BuffersQueued

    @property
    def samples_played(self):
        """Get the amount of samples played by the voice."""
        self._voice.GetState(byref(self._voice_state), 0)
        # _debug(f"XA2SourceVoice.samples_played: {self._voice_state.SamplesPlayed + self.phantom_samples_played} ({perf_counter()})")
        return self._voice_state.SamplesPlayed + self.phantom_samples_played

    @property
    def volume(self):
        vol = c_float()
        self._voice.GetVolume(ctypes.byref(vol))
        return vol.value

    @volume.setter
    def volume(self, value):
        self._voice.SetVolume(value, 0)

    @property
    def is_emitter(self):
        return self._emitter is not None

    @property
    def position(self):
        if self.is_emitter:
            return self._emitter.Position.x, self._emitter.Position.y, self._emitter.Position.z
        else:
            return 0, 0, 0

    @position.setter
    def position(self, position):
        if self.is_emitter:
            x, y, z = position
            self._emitter.Position.x = x
            self._emitter.Position.y = y
            self._emitter.Position.z = z

    @property
    def min_distance(self):
        """Curve distance scaler that is used to scale normalized distance curves to user-defined world units,
        and/or to exaggerate their effect."""
        if self.is_emitter:
            return self._emitter.CurveDistanceScaler
        else:
            return 0

    @min_distance.setter
    def min_distance(self, value):
        if self.is_emitter:
            if self._emitter.CurveDistanceScaler != value:
                self._emitter.CurveDistanceScaler = min(value, lib.FLT_MAX)

    @property
    def frequency(self):
        """The actual frequency ratio. If voice is 3d enabled, will be overwritten next apply3d cycle."""
        value = c_float()
        self._voice.GetFrequencyRatio(byref(value))
        return value.value

    @frequency.setter
    def frequency(self, value):
        if self.frequency == value:
            return

        self._voice.SetFrequencyRatio(value, 0)

    @property
    def cone_orientation(self):
        """The orientation of the sound emitter."""
        if self.is_emitter:
            return self._emitter.OrientFront.x, self._emitter.OrientFront.y, self._emitter.OrientFront.z
        else:
            return 0, 0, 0

    @cone_orientation.setter
    def cone_orientation(self, value):
        if self.is_emitter:
            x, y, z = value
            self._emitter.OrientFront.x = x
            self._emitter.OrientFront.y = y
            self._emitter.OrientFront.z = z

    _ConeAngles = namedtuple('_ConeAngles', ['inside', 'outside'])

    @property
    def cone_angles(self):
        """The inside and outside angles of the sound projection cone."""
        if self.is_emitter:
            return self._ConeAngles(self._emitter.pCone.contents.InnerAngle, self._emitter.pCone.contents.OuterAngle)
        else:
            return self._ConeAngles(0, 0)

    def set_cone_angles(self, inside, outside):
        """The inside and outside angles of the sound projection cone."""
        if self.is_emitter:
            self._emitter.pCone.contents.InnerAngle = inside
            self._emitter.pCone.contents.OuterAngle = outside

    @property
    def cone_outside_volume(self):
        """The volume scaler of the sound beyond the outer cone."""
        if self.is_emitter:
            return self._emitter.pCone.contents.OuterVolume
        else:
            return 0

    @cone_outside_volume.setter
    def cone_outside_volume(self, value):
        if self.is_emitter:
            self._emitter.pCone.contents.OuterVolume = value

    @property
    def cone_inside_volume(self):
        """The volume scaler of the sound within the inner cone."""
        if self.is_emitter:
            return self._emitter.pCone.contents.InnerVolume
        else:
            return 0

    @cone_inside_volume.setter
    def cone_inside_volume(self, value):
        if self.is_emitter:
            self._emitter.pCone.contents.InnerVolume = value

    def flush(self):
        """Stop and removes all buffers already queued. OnBufferEnd is called for each."""
        self._voice.Stop(0, 0)
        self._voice.FlushSourceBuffers()

    def play(self):
        self._voice.Start(0, 0)
        self._playing = True

    def stop(self):
        self._voice.Stop(0, 0)
        self._playing = False

    def submit_audio_data(self, audio_data):
        """Submit a piece of AudioData to the voice by creating a XAUDIO2_BUFFER from its pointer.
        This will create a reference to the AudioData in `audio_data_in_use`.
        Note that `audio_data_in_use` is not modified by any other methods of the `XA2SourceVoice`.
        You want to remove those explicitly in an `OnBufferEnd` callback.
        """
        xa2_buf = create_xa2_buffer(audio_data)

        with self._buffer_lock:
            self.audio_data_in_use.append((self._absolute_submitted_frame_count, audio_data))
            self._absolute_submitted_frame_count += audio_data.length // self._bytes_per_frame
            self._voice.SubmitSourceBuffer(byref(xa2_buf), None)

    def create_fake_voice(self, driver):
        # Driver is not playing at the time this method is called, but samples_played and
        # other voice state can still be queried.

        af = AudioFormat(self.channel_count, self.sample_size, self._sample_rate)
        print("    ", af)
        # Blind replacing of actual XAudio voices is okay.
        # Deleting the true driver will still free them; no call to Release is necessary.
        self.phantom_samples_played = self.samples_played
        # TODO: Thread switch here can cause the higher AudioPlayer's play cursor to exceed write cursor!
        # __import__("time").sleep(0.5)
        self._voice = driver._create_new_fake_voice(af,
                                                    self._callback,
                                                    self._buffer_lock,
                                                    self.audio_data_in_use,
                                                    self.phantom_samples_played,
                                                    self._absolute_submitted_frame_count,
                                                    self._playing)

    def create_real_voice_and_restart(self, fmt, driver):
        """Restart the voice. Called after it has been recreated after a driver dropout.
        """
        self.phantom_samples_played = self.samples_played
        self._voice = driver._create_new_real_voice(fmt, self._callback)

        if not self.audio_data_in_use:
            return

        if self.audio_data_in_use:
            # Attempt to submit the first buffer with a PlayBegin value so that it starts out at samples_played.
            first_buf_start, ad = self.audio_data_in_use[0]
            frames = ad.length // self._bytes_per_frame

            ideal_start = self.phantom_samples_played - first_buf_start
            if ideal_start >= frames:
                # First buffer is already played? shouldn't really happen
                # TODO might happen when threading and flushing go wrong
                _debug(f"FIXME First buffer is exhausted already? {ideal_start=} {frames=}")
            elif ideal_start < 0:
                _debug(f"ideal_start was negative? {ideal_start=} {frames=}")
            else:
                buf = create_xa2_buffer(ad)
                buf.PlayBegin = ideal_start
                self._voice.SubmitSourceBuffer(byref(buf), None)

        for i in range(1, len(self.audio_data_in_use)):
            self._voice.SubmitSourceBuffer(byref(create_xa2_buffer(self.audio_data_in_use[i][1])), None)

        if self._playing:
            self._voice.Start(0, 0)


class XAudio2Listener:
    def __init__(self, driver):
        self.xa2_driver = weakref.proxy(driver)
        self.listener = lib.X3DAUDIO_LISTENER()

        # Default listener orientations for DirectSound/XAudio2:
        # Front: (0, 0, 1), Up: (0, 1, 0)
        self.listener.OrientFront.x = 0
        self.listener.OrientFront.y = 0
        self.listener.OrientFront.z = 1

        self.listener.OrientTop.x = 0
        self.listener.OrientTop.y = 1
        self.listener.OrientTop.z = 0

    def delete(self):
        self.listener = None

    @property
    def position(self):
        return self.listener.Position.x, self.listener.Position.y, self.listener.Position.z

    @position.setter
    def position(self, value):
        x, y, z = value
        self.listener.Position.x = x
        self.listener.Position.y = y
        self.listener.Position.z = z

    @property
    def orientation(self):
        return self.listener.OrientFront.x, self.listener.OrientFront.y, self.listener.OrientFront.z, \
               self.listener.OrientTop.x, self.listener.OrientTop.y, self.listener.OrientTop.z

    @orientation.setter
    def orientation(self, orientation):
        front_x, front_y, front_z, top_x, top_y, top_z = orientation

        self.listener.OrientFront.x = front_x
        self.listener.OrientFront.y = front_y
        self.listener.OrientFront.z = front_z

        self.listener.OrientTop.x = top_x
        self.listener.OrientTop.y = top_y
        self.listener.OrientTop.z = top_z
