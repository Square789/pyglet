from __future__ import annotations

from math import ceil
import threading
from time import perf_counter, sleep
import weakref
from collections import defaultdict, deque, namedtuple
from ctypes import POINTER, byref, c_char, c_float, cast, pointer
from ctypes.wintypes import DWORD, FLOAT

import pyglet
from pyglet.util import debug_print
from pyglet.libs.win32 import com
from pyglet.libs.win32.com import S_OK
from pyglet.media.codecs.base import AudioFormat
from pyglet.media.devices import get_audio_device_manager
from pyglet.media.devices.base import DeviceFlow
from pyglet.util import debug_print
from . import lib_xaudio2 as lib

_debug = debug_print('debug_media')


def create_xa2_buffer(audio_data, play_begin=0):
    """Creates a XAUDIO2_BUFFER to be used with a source voice.
        Audio data cannot be purged until the source voice has played it; doing so will cause glitches."""
    buff = lib.XAUDIO2_BUFFER()
    buff.AudioBytes = audio_data.length
    buff.pAudioData = cast(audio_data.pointer, POINTER(c_char))
    buff.PlayBegin = play_begin
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


class XA2EngineCallback(com.COMObject):
    _interfaces_ = [lib.IXAudio2EngineCallback]

    def __init__(self, driver):
        super().__init__()
        self._driver = driver

    def OnProcessingPassStart(self):
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


class XAudio2Listener:
    def __init__(self) -> None:
        self._listener = lib.X3DAUDIO_LISTENER()

        # Default listener orientations for DirectSound/XAudio2:
        # Front: (0, 0, 1), Up: (0, 1, 0)
        self._listener.OrientFront.x = 0
        self._listener.OrientFront.y = 0
        self._listener.OrientFront.z = 1

        self._listener.OrientTop.x = 0
        self._listener.OrientTop.y = 1
        self._listener.OrientTop.z = 0

    @property
    def position(self):
        return self._listener.Position.x, self._listener.Position.y, self._listener.Position.z

    @position.setter
    def position(self, value):
        x, y, z = value
        self._listener.Position.x = x
        self._listener.Position.y = y
        self._listener.Position.z = z

    @property
    def orientation(self):
        return self._listener.OrientFront.x, self._listener.OrientFront.y, self._listener.OrientFront.z, \
               self._listener.OrientTop.x, self._listener.OrientTop.y, self._listener.OrientTop.z

    @orientation.setter
    def orientation(self, orientation):
        front_x, front_y, front_z, top_x, top_y, top_z = orientation

        self._listener.OrientFront.x = front_x
        self._listener.OrientFront.y = front_y
        self._listener.OrientFront.z = front_z

        self._listener.OrientTop.x = top_x
        self._listener.OrientTop.y = top_y
        self._listener.OrientTop.z = top_z


class XAudio2Emitter:
    def __init__(
        self,
        channel_count: int,
        curve_distance_scaler: float,
        cone_inner_angle: float,
        cone_outer_angle: float,
        cone_inner_volume: float,
        cone_outer_volume: float,
    ) -> None:
        self._cone = lib.X3DAUDIO_CONE(
            cone_inner_angle,
            cone_outer_angle,
            cone_inner_volume,
            cone_outer_volume,
        )
        self._emitter = lib.X3DAUDIO_EMITTER()
        self._emitter.pCone = pointer(self._cone)
        self._emitter.ChannelCount = channel_count
        self._emitter.CurveDistanceScaler = curve_distance_scaler

    @property
    def curve_distance_scaler(self) -> float:
        return self._emitter.CurveDistanceScaler

    @curve_distance_scaler.setter
    def curve_distance_scaler(self, v: float) -> None:
        self._emitter.CurveDistanceScaler = v

    @property
    def channel_count(self) -> float:
        return self._emitter.ChannelCount

    @channel_count.setter
    def channel_count(self, v: float) -> None:
        self._emitter.ChannelCount = v

    @property
    def position(self) -> tuple[float, float, float]:
        p = self._emitter.Position
        return p.x, p.y, p.z

    @position.setter
    def position(self, v: tuple[float, float, float]) -> None:
        x, y, z = v
        self._emitter.Position.x = x
        self._emitter.Position.y = y
        self._emitter.Position.z = z

    @property
    def cone_orientation(self) -> tuple[float, float, float]:
        p = self._emitter.OrientFront
        return p.x, p.y, p.z

    @cone_orientation.setter
    def cone_orientation(self, v: tuple[float, float, float]) -> None:
        x, y, z = v
        self._emitter.OrientFront.x = x
        self._emitter.OrientFront.y = y
        self._emitter.OrientFront.z = z

    @property
    def cone_outer_volume(self) -> float:
        """The volume scaler of the sound beyond the outer cone."""
        return self._cone.OuterVolume

    @cone_outer_volume.setter
    def cone_outer_volume(self, v: float) -> None:
        self._cone.OuterVolume = v


# ===== XAudio2 FUNCTIONALITY MOVEMENT PATCH =====

class X3DAudio:
    def __init__(
        self,
        mvoice_channel_mask: int,
        mvoice_input_channel_count: int,
        speed_of_sound: float = lib.X3DAUDIO_SPEED_OF_SOUND,
        calculation_flags: int = lib.X3DAUDIO_CALCULATE_MATRIX,
    ) -> None:
        self._handle = lib.X3DAUDIO_HANDLE()

        self._dsp_settings = lib.X3DAUDIO_DSP_SETTINGS()
        self._dsp_settings_matrix_coefficients = None

        self.reinitialize(mvoice_channel_mask, mvoice_input_channel_count, speed_of_sound, calculation_flags)

    def reinitialize(
        self,
        mvoice_channel_mask: int,
        mvoice_input_channel_count: int,
        speed_of_sound: float = lib.X3DAUDIO_SPEED_OF_SOUND,
        calculation_flags: int = lib.X3DAUDIO_CALCULATE_MATRIX,
    ) -> None:
        self._calculation_flags = calculation_flags

        self._dsp_settings_matrix_coefficients = (FLOAT * mvoice_input_channel_count)()
        self._dsp_settings.SrcChannelCount = 1
        self._dsp_settings.DstChannelCount = mvoice_input_channel_count
        self._dsp_settings.pMatrixCoefficients = self._dsp_settings_matrix_coefficients
        self._dsp_settings.pDelayTimes = None

        lib.X3DAudioInitialize(mvoice_channel_mask, speed_of_sound, self._handle)

    def calculate(self, listener, emitter) -> lib.X3DAUDIO_DSP_SETTINGS:
        lib.X3DAudioCalculate(self._handle, listener, emitter, self._calculation_flags, self._dsp_settings)
        return self._dsp_settings


# TODO: This needs to become an XAudioEngineGate, no way around it really.

class XAudio2Engine:
    # Max Frequency a voice can have. Setting this higher/lower will increase/decrease memory allocation.
    max_frequency_ratio = 2.0

    def __init__(
        self,
        callback,
        device_id=None,
        processor=lib.XAUDIO2_DEFAULT_PROCESSOR,
        category=lib.AudioCategory_GameEffects,
    ) -> None:
        self._engine_callback = callback
        self._device_id = device_id
        self._processor = processor
        self._category = category

        self._xaudio2 = lib.IXAudio2()
        self._master_voice = lib.IXAudio2MasteringVoice()
        self.master_voice_channel_mask = None

        try:
            lib.XAudio2Create(byref(self._xaudio2), 0, processor)
        except OSError:
            raise

        if _debug:
            # Debug messages are found in Windows Event Viewer, you must enable event logging:
            # Applications and Services -> Microsoft -> Windows -> Xaudio2 -> Debug Logging.
            # Right click -> Enable Logs
            debug = lib.XAUDIO2_DEBUG_CONFIGURATION()
            debug.LogThreadID = True
            debug.TraceMask = lib.XAUDIO2_LOG_ERRORS | lib.XAUDIO2_LOG_WARNINGS
            debug.BreakMask = lib.XAUDIO2_LOG_WARNINGS

            self._xaudio2.SetDebugConfiguration(byref(debug), None)

        self._xaudio2.RegisterForCallbacks(self._engine_callback)

        try:
            self._xaudio2.CreateMasteringVoice(byref(self._master_voice),
                                            lib.XAUDIO2_DEFAULT_CHANNELS,
                                            lib.XAUDIO2_DEFAULT_SAMPLERATE,
                                            0, None, None, self._category)
        except OSError:
            self._xaudio2.Release()
            raise

        mvoice_details = lib.XAUDIO2_VOICE_DETAILS()
        self._master_voice.GetVoiceDetails(byref(mvoice_details))
        self.master_voice_input_channel_count = mvoice_details.InputChannels

        channel_mask = DWORD()
        self._master_voice.GetChannelMask(byref(channel_mask))
        self.master_voice_channel_mask = channel_mask.value

    def get_performance(self) -> lib.XAUDIO2_PERFORMANCE_DATA:
        """Retrieve some basic XAudio2 performance data such as memory usage and source counts."""
        pf = lib.XAUDIO2_PERFORMANCE_DATA()
        self._xaudio2.GetPerformanceData(byref(pf))
        return pf

    def delete(self) -> None:
        self._xaudio2.UnregisterForCallbacks(self._engine_callback)
        self._xaudio2.StopEngine()
        self._xaudio2.Release()

    def apply3d(
        self,
        source_voice: XAudio2SourceVoiceGate,
        dsp_settings: lib.X3DAUDIO_DSP_SETTINGS,
        commit: int,
    ) -> None:
        """Apply results of an X3DAudio calculation to the given voice."""
        source_voice._voice.SetOutputMatrix(
            self._master_voice,
            1,
            self.master_voice_input_channel_count,
            dsp_settings.pMatrixCoefficients,
            commit,
        )

        # TODO: Change, this overwrites pitch otherwise!
        # TODO TODO: DopplerFactor will always be 1 as pyglet doesn't do acceleration. Remove!
        # TODO TODO TODO: remove lib.default_dsp_calculation in same breath.
        # source_voice._voice.SetFrequencyRatio(dsp_settings.DopplerFactor * source_voice.frequency, commit)

    def create_source_voice(self, audio_format: AudioFormat, callback: XAudio2VoiceCallback) -> lib.IXAudio2SourceVoice:
        voice_struct = lib.IXAudio2SourceVoice()
        wfx = create_xa2_waveformat(audio_format)

        self._xaudio2.CreateSourceVoice(
            byref(voice_struct),
            byref(wfx),
            0,
            self.max_frequency_ratio,
            callback,
            None,
            None,
        )

        return voice_struct


class XAudio2SourceVoiceGate:
    # Thin wrapper over either a IXAudio2SourceVoice or FakeXAudio2Voice
    # The goal of interface.py files was probably to minimize ctypes interaction, but
    # that convention is not set in stone.

    def __init__(self, voice, callback, pool_key) -> None:
        # This is technically not thread-safe, but many pyglet audio routines aren't.
        # Think about it really hard then come to a conclusion
        # Thought about it really hard. These are barebones interfaces and should not have any
        # locks on them.
        # TODO: In fact, they also shouldn't have all of this, move back to resetter helper classes!
        self._voice_state = lib.XAUDIO2_VOICE_STATE()
        self._voice = voice
        self.callback = callback
        self.samples_played_at_last_recycle = 0
        self.pool_key = pool_key
        self.audio_data_during_reset = []

    def submit_audio_data(self, audio_data, play_begin=0):
        xa2_buf = create_xa2_buffer(audio_data, play_begin)
        self._voice.SubmitSourceBuffer(byref(xa2_buf), None)

    def destroy(self):
        """Completely destroy the voice."""
        self._emitter = None

        if self._voice is not None:
            self._voice.DestroyVoice()
            self._voice = None

        self.callback = None

    def _set_sample_rate(self, sr: float):
        self._voice.SetSourceSampleRate(sr)

    sample_rate = property(None, _set_sample_rate)

    @property
    def buffers_queued(self):
        """Get the amount of buffers in the current voice."""
        self._voice.GetState(byref(self._voice_state), lib.XAUDIO2_VOICE_NOSAMPLESPLAYED)
        # _debug(f"XA2SourceVoice.buffers_queued: {self._voice_state.BuffersQueued}")
        return self._voice_state.BuffersQueued

    @property
    def samples_played(self):
        """Get the amount of samples played by the voice.
        Note that while XAudio2 refers to them as samples, the pyglet term would be frames.
        """
        self._voice.GetState(byref(self._voice_state), 0)
        # _debug(f"XA2SourceVoice.samples_played: {self._voice_state.SamplesPlayed} ({perf_counter()})")
        return self._voice_state.SamplesPlayed

    @property
    def volume(self):
        vol = c_float()
        self._voice.GetVolume(byref(vol))
        return vol.value

    @volume.setter
    def volume(self, value):
        self._voice.SetVolume(value, 0)

    # TODO: Multiply with this in apply3d how about that
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

    def flush(self):
        """Stop and removes all buffers already queued. OnBufferEnd is called for each."""
        self._voice.FlushSourceBuffers()

    # TODO: There used to be locks here, as well as in samples_played.get and buffer_queued.get.
    # Move those in back to the XAudio2Player.
    def play(self):
        self._voice.Start(0, 0)

    def stop(self):
        self._voice.Stop(0, 0)

    def set_output_matrix():
        return
