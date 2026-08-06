from __future__ import annotations

from collections import defaultdict, deque
import math
import threading
from time import perf_counter
from typing import Deque, Tuple, TYPE_CHECKING

import pyglet
from pyglet.media.codecs import AudioData, Source
from pyglet.media.devices import get_audio_device_manager
from pyglet.media.devices.base import DeviceFlow
from pyglet.media.drivers.base import AbstractAudioDriver, AbstractAudioPlayer, MediaEvent
from pyglet.media.player_worker_thread import PlayerWorkerThread
from pyglet.media.drivers.listener import AbstractListener
from pyglet.util import debug_print
from . import fake
from . import interface

if TYPE_CHECKING:
    from pyglet.media.player import Player
    from pyglet.media.codecs.base import AudioFormat

VoiceKey = tuple[int, int]


_debug = debug_print('debug_media')


def _convert_coordinates(coordinates: Tuple[float, float, float]) -> Tuple[float, float, float]:
    x, y, z = coordinates
    return x, y, -z


class XAudio2EngineGate:
    def __init__(self) -> None:
        self._engine = None


class XAudio2Driver(AbstractAudioDriver):
    def __init__(self) -> None:
        self._listener = XAudio2Listener()

        self.lock = threading.Lock()
        """
        A lock held while the audio engine is in a processing step.
        """

        self._engine_callback = interface.XA2EngineCallback(self)

        self.max_frequency_ratio = 2.0

        self._fake_engine = fake.FakeXAudio2Engine(self, self.max_frequency_ratio)
        self._real_engine = None

        self._using_fake_engine = False
        self._default_device_gone = False
        self._time_of_dropout = 0.0

        self._voice_pool_lock = threading.Lock()
        self._voice_pool: defaultdict[VoiceKey, list[interface.XAudio2SourceVoiceGate]] = defaultdict(list)
        self._voices_in_use: dict[interface.XAudio2SourceVoiceGate, XAudio2AudioPlayer] = {}
        self._voices_resetting: set[interface.XAudio2SourceVoiceGate] = set()
        self._voices_emitting: list[interface.XAudio2SourceVoiceGate] = []

        try:
            self._setup_real_engine()
        except OSError:
            self._default_device_gone = True
            self._using_fake_engine = True
            self._fake_engine.start(self._time_of_dropout, [])

        # TODO: gross sidestep
        engine = self._fake_engine if self._using_fake_engine else self._real_engine
        self._x3daudio = interface.X3DAudio(engine.master_voice_channel_mask, engine.master_voice_input_channel_count)

        # TODO: Unscheduling a function from the clock doesn't guarantee it still won't be called afterwards.
        # The clock really isn't threadsafe. Fix that.

        if (device_mgr := get_audio_device_manager()) is not None:
            assert _debug("Audio device manager instance found.")
            device_mgr.push_handlers(self)
            pyglet.clock.schedule_interval_soft(self._check_state, 0.5)

        self.worker = PlayerWorkerThread()
        self.worker.start()

    def on_default_changed(self, device, flow):
        """Callback derived from the Audio Devices to help us determine when the system no longer has output."""
        if flow != DeviceFlow.OUTPUT:
            return

        if device is None:
            assert _debug('Error: Default audio device was removed or went missing.')
            self._time_of_dropout = perf_counter()
            self._default_device_gone = True
        elif self._default_device_gone:
            assert _debug('Warning: Default audio device added after going missing.')
            self._default_device_gone = False

    def _check_state(self, _dt) -> None:
        if self._default_device_gone:
            if not self._using_fake_engine:
                self._using_fake_engine = True

                assert _debug("XAudio2Driver._check_state: shutting down")

                # Exchange all existing voices for fake voices.
                _debug(f"Installing fake voices {self._voice_pool} {self._voices_in_use}")
                self._x3daudio.reinitialize(
                    self._fake_engine.master_voice_channel_mask,
                    self._fake_engine.master_voice_input_channel_count,
                )

                with self._voice_pool_lock:
                    self._reset_resetting_voices()

                    self._voice_pool.clear()

                    for player in self._voices_in_use.values():
                        player.create_fake_voice()

                self._delete_real_engine(destroy_voices=False)
                for i in self._voices_in_use:
                    print(i, i._voice)
                    if not isinstance(i._voice, fake.FakeXAudio2SourceVoice):
                        raise RuntimeError()
                self._fake_engine.start(self._time_of_dropout, [g._voice for g in self._voices_in_use])

        else:
            if self._using_fake_engine:
                self._using_fake_engine = False

                assert _debug("XAudio2Driver._check_state: recreating and resetting")
                self._setup_real_engine()

                self._x3daudio.reinitialize(
                    self._real_engine.master_voice_channel_mask,
                    self._real_engine.master_voice_input_channel_count,
                )

                _debug("Stopping fake driver")
                # NOTE: Will wait on a thread. Probably not too serious.
                self._fake_engine.stop()

                _debug(f"Recreating real voices {self._voice_pool} {self._voices_in_use}")
                with self._voice_pool_lock:
                    # Flushing voices won't have OnBufferEnd callbacks called anymore.
                    # Flush them instantly here to update the voice gates, plugging in new
                    # voices should be good then.
                    self._reset_resetting_voices()
                    assert not self._voices_resetting

                    self._voice_pool.clear()

                    for player in self._voices_in_use.values():
                        player.create_real_voice()


    def _setup_real_engine(self) -> None:
        assert self._real_engine is None
        self._real_engine = interface.XAudio2Engine(self._engine_callback)

    def _commit_changes(self, commit_set_id) -> None:
        # TODO Gross sidestep
        if self._using_fake_engine:
            self._fake_engine.CommitChanges(commit_set_id)
        else:
            self._real_engine._xaudio2.CommitChanges(commit_set_id)

    def _calculate_3d_sources(self, dt) -> None:
        for gate in self._voices_emitting:
            self._calculate_and_apply_3d(self._voices_in_use[gate], 1)

        self._commit_changes(1)

    def apply3d(self, source_voice):
        """Apply and immediately commit positional audio effects for the given voice."""
        self._calculate_and_apply_3d(source_voice, 2)
        self._commit_changes(2)

    def _calculate_and_apply_3d(self, source_voice, commit):
        """Calculates and sets output matrix and frequency ratio on the voice based on the listener and the voice's
        emitter. Commit determines the operation set, whether the settings are applied immediately (0) or to
        be committed together at a later time.
        """
        if self._using_fake_engine:
            # Fake engine has no output and the calculations do not change the voice's frequency or
            # apply any other time-dilating effects, should be safe to skip it.
            # TODO: Absolutely not thread-safe, figure out
            return

        dsp_settings = self._x3daudio.calculate(self._listener._xa2_listener, source_voice.emitter)
        self._real_engine.apply3d(source_voice._voice, dsp_settings, commit)

    def _delete_real_engine(self, destroy_voices=True):
        if self._real_engine is None:
            return

        assert _debug("XAudio2Driver: Deleting")

        # Stop 3d
        pyglet.clock.unschedule(self._calculate_3d_sources)
        self._dsp_settings = None

        # Destroy all pooled voices as master will change.
        if destroy_voices:
            self._destroy_voices()

        self._real_engine.delete()
        self._real_engine = None

    def _destroy_voices(self):
        """Destroy and clear all voice pools."""
        for list_ in self._voice_pool.values():
            for voice in list_:
                voice.destroy()
            list_.clear()

        for voice in self._voices_resetting:
            voice.destroy()
        self._voices_resetting.clear()

        self._voices_emitting.clear()
        for voice in self._voices_in_use.keys():
            voice.destroy()
        self._voices_in_use.clear()

    def _reset_resetting_voices(self):
        for v in self._voices_resetting:
            self._return_reset_voice_gate(v)

    def _reset_voice_gate_on_buffer_end(self, voice_gate) -> None:
        if voice_gate.buffers_queued == 0:
            # Due to some asynchronity when calling Stop(0, 0), we can only be reasonably sure
            # a voice is stopped and ready for getting repooled right here.
            self._return_reset_voice_gate(voice_gate)

    def _return_reset_voice_gate(self, voice_gate) -> None:
        # Another thread can interfere here.
        # This might lead to the voice getting overlooked during a driver swapout or to inconsistencies in
        # some of its attributes.
        # However, this function is called either while _voice_pool_lock is held, or from within an
        # XAudio2 callback, where a swapout won't happen.
        samples_played = voice_gate.samples_played
        voice_gate.audio_data_during_reset.clear()
        voice_gate.callback.on_buffer_end = None
        voice_gate.samples_played_at_last_recycle = samples_played
        self._voices_resetting.remove(voice_gate)
        self._voice_pool[voice_gate.pool_key].append(voice_gate)
        assert _debug(f"XA2AudioDriver: {voice_gate} back in pool")

    def return_voice_gate(self, voice_gate: "interface.XAudio2SourceVoiceGate", audio_data) -> None:
        """Reset a voice gate and eventually return it to the pool.
        The voice gate must be stopped and must have been handed out by the driver beforehand."""
        with self._voice_pool_lock:
            try:
                self._voices_emitting.remove(voice_gate)
            except ValueError:
                pass
            self._voices_in_use.pop(voice_gate)

            assert _debug(f"XA2AudioDriver: Resetting {voice_gate}...")

            self._voices_resetting.add(voice_gate)
            if voice_gate.buffers_queued != 0:
                # If the audio thread ran right here and we would now be at zero buffers,
                # the callback would never be invoked.
                # For this reason, we explicitly check again below.
                voice_gate.callback.on_buffer_end = lambda *_, v=voice_gate: self._reset_voice_gate_on_buffer_end(v)
                voice_gate.audio_data_during_reset = audio_data
                voice_gate.flush()
                if voice_gate.buffers_queued == 0:
                    # We have definitely been interrupted. That's not good.
                    # A: Maybe the python callback has already run.
                    # B: Maybe the python callback is running RIGHT now. (very unlikely but not impossible.
                    #    I believe callbacks from native code can be paused in favor of other threads)
                    # C: Maybe the python callback will run soon.
                    # D: Maybe the python callback will never run.
                    # D is a possibility, grab the engine lock to ensure it's not B and return the voice
                    # if it wasn't already.
                    with self.lock:
                        if voice_gate in self._voices_resetting:
                            self._return_reset_voice_gate(voice_gate)
            else:
                self._return_reset_voice_gate(voice_gate)

    def get_voice_gate(self, audio_format: 'AudioFormat', player: 'XAudio2AudioPlayer') -> 'interface.XAudio2SourceVoiceGate':
        voice_key = (audio_format.channels, audio_format.sample_size)

        with self._voice_pool_lock:
            if not self._voice_pool[voice_key]:
                voice_gate = self._create_new_voice_gate(audio_format)
                # Create a 2nd one for good measure, multiple players might be needing it soon,
                # and a clear command will probably complete more quickly when swapping out for a
                # pooled voice
                self._voice_pool[voice_key].append(self._create_new_voice_gate(audio_format))
            else:
                voice_gate = self._voice_pool[voice_key].pop()

            # HACK: duplicating the `channels == 1`` condition
            if audio_format.channels == 1:
                self._voices_emitting.append(voice_gate)
            self._voices_in_use[voice_gate] = player

        return voice_gate

    def _create_new_voice_gate(self, audio_format: 'AudioFormat') -> 'interface.XAudio2SourceVoiceGate':
        # either the voice_pool_lock must be held when calling this method
        # or the engine must be stopped

        callback = interface.XAudio2VoiceCallback()
        if self._using_fake_engine:
            voice = self.create_fake_voice(audio_format, callback)
        else:
            # TODO: This could fail when the real engine errored out but the fake engine has
            # not been set up yet.
            voice = self.create_real_voice(audio_format, callback)

        # HACK repetition of pool key, only so it's available on reset
        return interface.XAudio2SourceVoiceGate(voice, callback, (audio_format.channels, audio_format.sample_size))

    def create_fake_voice(
        self,
        audio_format: 'AudioFormat',
        callback: interface.XAudio2VoiceCallback,
        buffer_lock: threading.Lock | None = None,
        initial_audio_data = (),
        first_buffer_offset = 0,
    ):
        """
        Create a fake voice gate from the fake engine.
        """
        if buffer_lock is None:
            buffer_lock = threading.Lock()

        return self._fake_engine.create_source_voice(audio_format, callback, buffer_lock, initial_audio_data, first_buffer_offset)

    def create_real_voice(self, audio_format: 'AudioFormat', callback: interface.XAudio2VoiceCallback):
        return self._real_engine.create_source_voice(audio_format, callback)

    def create_audio_player(self, source: 'Source', player: 'Player') -> 'XAudio2AudioPlayer':
        return XAudio2AudioPlayer(self, source, player)

    def get_listener(self) -> 'XAudio2Listener':
        return self._listener

    def delete(self) -> None:
        if self._real_engine is not None:
            self.worker.stop()
            self.worker = None
            get_audio_device_manager().remove_handlers(self)
            self._real_engine.delete()
            self._real_engine = None
            self._listener = None


class XAudio2Listener(AbstractListener):
    def __init__(self) -> None:
        self._xa2_driver = None
        self._xa2_listener = None

    def connect(self, xa2_driver, xa2_listener) -> None:
        self._xa2_driver = xa2_driver
        self._xa2_listener = xa2_listener
        self._set_volume(self._volume)
        self._set_position(self._position)
        self._set_orientation()

    def disconnect(self) -> None:
        self._xa2_driver = self._xa2_listener = None

    def _set_volume(self, volume: float) -> None:
        self._volume = volume
        if self._xa2_driver is not None:
            self._xa2_driver.volume = volume

    def _set_position(self, position: Tuple[float, float, float]) -> None:
        self._position = position
        if self._xa2_listener is not None:
            self._xa2_listener.position = _convert_coordinates(position)

    def _set_forward_orientation(self, orientation: Tuple[float, float, float]) -> None:
        self._forward_orientation = orientation
        self._set_orientation()

    def _set_up_orientation(self, orientation: Tuple[float, float, float]) -> None:
        self._up_orientation = orientation
        self._set_orientation()

    def _set_orientation(self) -> None:
        if self._xa2_listener is not None:
            self._xa2_listener.orientation = (_convert_coordinates(self._forward_orientation) +
                                              _convert_coordinates(self._up_orientation))


class XAudio2AudioPlayer(AbstractAudioPlayer):
    def __init__(self, driver: 'XAudio2Driver', source: 'Source', player: 'Player') -> None:
        super().__init__(source, player)
        # We keep here a strong reference because the AudioDriver is anyway
        # a singleton object which will only be deleted when the application
        # shuts down. The AudioDriver does not keep a ref to the AudioPlayer.
        self.driver = driver

        self._audio_format = source.audio_format # type: AudioFormat

        # Need to cache these because pyglet API allows update separately, but
        # XAudio2 requires both to be set at once.
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

        # Samples added onto a voice gate's reported `samples_played` in case its underlying
        # voice got replaced. Used for XAudio2 fake driver replacement/recreation.
        self._phantom_samples_played = 0

        # How many frames have been submitted to the voice gate. Used to keep track of buffer
        # starts and to calculate a PlayBegin offset for XAudio2 fake driver replacement/recreatiion.
        self._absolute_submitted_frame_count = 0
        self._audio_data_in_use: Deque['tuple[int, AudioData]'] = deque()

        if self._audio_format.channels == 1:
            self._emitter = interface.XAudio2Emitter(self._audio_format.channels, 1.0)
        else:
            self._emitter = None

        self._pyglet_source_exhausted = False

        # A lock to be held whenever modifying things relating to the in-use audio data,
        # as well as when submitting audio data to the underlying voice.
        # Ensures that the XAudio2 callbacks will not interfere with the
        # player operations.
        self._audio_data_lock = threading.Lock()

        # TODO: Quick garbage lock. Can be refined.
        # Purpose is to protect interaction with the voice during driver swapout operations.
        self._lock = threading.Lock()

        self._get_and_configure_voice_gate()

    def delete(self) -> None:
        # TODO: determine alternative method  and stuff
        if self._voice_gate is None or self.driver._real_engine is None:
            assert _debug("Xaudio2: Player deleted, driver or voice is gone")
            # Driver was deleted; just break up some references and return
            self.driver = None
            self._voice_gate = None
            return

        assert _debug("XAudio2: Player deleted, returning voice")

        self.stop()
        self.driver.return_voice_gate(self._voice_gate, self._audio_data_in_use)
        self.driver = None
        self._voice_gate = None

    def play(self) -> None:
        assert _debug(f'XAudio2 play: {self._playing=}')

        if not self._playing:
            with self._lock:
                self._playing = True
                if self._voice_gate is not None:
                    self._voice_gate.play()
                    self.driver.worker.add(self)

        assert _debug('return XAudio2 play')

    def stop(self) -> None:
        assert _debug('XAudio2 stop')

        if self._playing:
            with self._lock:
                if self._voice_gate is not None:
                    self.driver.worker.remove(self)
                    # no callback could possibly be running after this lock is released.
                    # TODO: evaluate closely how bad this is, actually
                    with self.driver.lock:
                        self._voice_gate.stop()
                self._playing = False

        assert _debug('return XAudio2 stop')

    def clear(self) -> None:
        assert _debug('XAudio2 clear')
        super().clear()
        self._play_cursor = 0
        self._write_cursor = 0
        self._phantom_samples_played = 0
        self._absolute_submitted_frame_count = 0
        self._pyglet_source_exhausted = False

        if self._voice_gate is not None:
            # XAudio2 voices can't be cleared immediately.
            # Swap it out for a new one instead of waiting for OnBufferEnd callbacks.
            self.driver.return_voice_gate(self._voice_gate, self._audio_data_in_use.copy())
            self._audio_data_in_use.clear()
            self._get_and_configure_voice_gate()

    def _get_and_configure_voice_gate(self) -> None:
        vg = self.driver.get_voice_gate(self.source.audio_format, self)

        vg.sample_rate = self._audio_format.sample_rate
        vg.callback.on_buffer_end = self.on_buffer_end

        self._voice_gate = vg

        vg.volume = self.player.volume
        vg.frequency = self.player.pitch
        if self._emitter is not None:
            self._emitter.position = _convert_coordinates(self.player.position)
            self._emitter.curve_distance_scaler = self.player.min_distance
            self._emitter.cone_orientation = _convert_coordinates(self.player.cone_orientation)
            self._emitter.cone_outer_volume = self.player.cone_outer_gain
            self._cone_inner_angle = self.player.cone_inner_angle
            self._cone_outer_angle = self.player.cone_outer_angle
            self._set_cone_angles()
            self.driver.apply3d(vg)

    def on_buffer_end(self, buffer_context_ptr: int) -> None:
        # Called from the XAudio2 thread.
        # A buffer stopped being played by the voice, it should by all means be the first one
        with self._audio_data_lock:
            assert self._audio_data_in_use
            self._audio_data_in_use.popleft()
            # This should cause the AudioData to lose all its references and be gc'd

            if self._audio_data_in_use:
                assert _debug(f"Buffer ended, {len(self._audio_data_in_use)} remaining")
                return

            assert self._voice_gate.buffers_queued == 0

            if self._pyglet_source_exhausted:
                # Last buffer ran out naturally, out of AudioData; voice will now fall silent
                assert _debug("Last buffer ended normally, dispatching eos")
                MediaEvent('on_eos').sync_dispatch_to_player(self.player)
            else:
                # Shouldn't have ran out; supplier is running behind
                # All we can do is wait; as long as voices are not stopped via `Stop`, they will
                # immediately continue playing the new buffer once it arrives
                assert _debug("Last buffer ended normally, source is lagging behind")

    def _replace_voice(self, create_real_voice):
        """
        Replace the currently installed voice after updating the
        `_phantom_samples_played` variable.
        """
        # This method may only be called at a very specific point where the XAudio2 engine
        # is not active.

        callback = self._voice_gate.callback

        # The real voice can be created before lock acquisition, it might be better to reduce
        # time spent in locks when creating foreign objects.
        new_voice = None
        if create_real_voice:
            new_voice = self.driver.create_real_voice(self._audio_format, callback)

        # Creating the new voice requires requeueing of all audio until now.
        # We cannot allow other threads to interfere there, so perform some very broad
        # locking.

        with self._lock:
            # When installing fake voices, the driver already errored out at this time,
            # but samples_played and other voice state can still be queried.
            _psp = self._phantom_samples_played
            self._phantom_samples_played += (self._voice_gate.samples_played - self._voice_gate.samples_played_at_last_recycle)
            _debug(f"phantom_samples_played: {_psp} -> {self._phantom_samples_played}")

            with self._audio_data_lock:
                if self._audio_data_in_use:
                    # Submit the first buffer with a PlayBegin value so that it starts out at samples_played.
                    first_buf_start, ad = self._audio_data_in_use[0]
                    frames = ad.length // self._audio_format.bytes_per_frame

                    ideal_start = self._phantom_samples_played - first_buf_start
                    _debug(f"{ideal_start=}, {self._phantom_samples_played=}, {self._voice_gate.samples_played=}, {first_buf_start=}")
                    if ideal_start >= frames:
                        _debug(f"FIXME First buffer is exhausted already? {ideal_start=} {frames=}")
                        ideal_start = -1
                    elif ideal_start < 0:
                        _debug(f"ideal_start was negative? {ideal_start=} {frames=}")
                        ideal_start = -1
                else:
                    ideal_start = -1

                if not create_real_voice:
                    # We need to grab the buffer lock, as a mutation of `audio_data_in_use` can
                    # cause unexpected problems in `FakeXAudio2Voice.__init__`.
                    new_voice = self.driver.create_fake_voice(self._audio_format,
                                                              callback,
                                                              self._audio_data_lock,
                                                              self._audio_data_in_use,
                                                              ideal_start)

                # This is extremely stupid, but whatever.
                # Blind replacement of real voices is okay: The true driver will still
                # free them; no call to Release is necessary.
                # Blind replacement of fake voices is okay as well, they'll be garbage collected.
                self._voice_gate._voice = new_voice
                self._voice_gate.samples_played_at_last_recycle = 0

                if create_real_voice and self._audio_data_in_use:
                    if ideal_start != -1:
                        self._voice_gate.submit_audio_data(ad, ideal_start)

                    for i in range(1, len(self._audio_data_in_use)):
                        self._voice_gate.submit_audio_data(self._audio_data_in_use[i][1])

            if self._playing:
                self._voice_gate.play()

    def create_fake_voice(self):
        self._replace_voice(False)

    def create_real_voice(self):
        self._replace_voice(True)

    def _refill(self, refill_size: int) -> None:
        """Get one piece of AudioData and submit it to the voice.
        This method will release the lock around the call to `get_audio_data`,
        so make sure it's held upon calling.
        """
        self._audio_data_lock.release()
        audio_data = self._get_and_compensate_audio_data(refill_size, self._play_cursor)
        self._audio_data_lock.acquire()

        if audio_data is None:
            assert _debug(f"XAudio2: Source is out of data")
            self._pyglet_source_exhausted = True
            if not self._audio_data_in_use:
                MediaEvent('on_eos').sync_dispatch_to_player(self.player)
            return

        self._voice_gate.submit_audio_data(audio_data)
        self._audio_data_in_use.append((self._absolute_submitted_frame_count, audio_data))
        self._absolute_submitted_frame_count += audio_data.length // self._audio_format.bytes_per_frame

        assert _debug(f"XAudio2: Submitted buffer of size {audio_data.length}B. {self._absolute_submitted_frame_count=}")

        self.append_events(self._write_cursor, audio_data.events)
        self._write_cursor += audio_data.length

    def _update_play_cursor(self) -> None:
        vg = self._voice_gate
        self._play_cursor = (
            (self._phantom_samples_played + vg.samples_played - vg.samples_played_at_last_recycle) *
            self.source.audio_format.bytes_per_frame
        )

    def get_play_cursor(self) -> int:
        return self._play_cursor

    def work(self) -> None:
        with self._audio_data_lock:
            self._update_play_cursor()
            self.dispatch_media_events(self._play_cursor)
            self._maybe_refill()

    def _maybe_refill(self) -> bool:
        if self._pyglet_source_exhausted:
            return False

        remaining_bytes = self._write_cursor - self._play_cursor
        if remaining_bytes >= self._buffered_data_comfortable_limit:
            # assert _debug(f"{remaining_bytes=}B (p@{self._play_cursor} w@{self._write_cursor})")
            return False

        missing_bytes = self._buffered_data_ideal_size - remaining_bytes
        assert _debug(f"Getting {missing_bytes}B of audio data, only {remaining_bytes}B remain. {self._write_cursor=}, {self._play_cursor=}")
        self._refill(self.source.audio_format.align_ceil(missing_bytes))

        return True

    def prefill_audio(self) -> None:
        if self._voice_gate is None:
            return

        with self._audio_data_lock:
            self._maybe_refill()

    def set_volume(self, volume: float) -> None:
        self._voice_gate.volume = volume

    def set_position(self, position: Tuple[float, float, float]) -> None:
        if self._emitter is not None:
            self._emitter.position = _convert_coordinates(position)

    def set_min_distance(self, min_distance: float) -> None:
        """Not a true min distance, but similar effect. Changes CurveDistanceScaler default is 1."""
        if self._emitter is not None:
            self._emitter.curve_distance_scaler = min_distance

    def set_max_distance(self, max_distance: float) -> None:
        """No such thing built into xaudio2"""
        pass

    def set_pitch(self, pitch: float) -> None:
        self._voice_gate.frequency = pitch

    def set_cone_orientation(self, cone_orientation: Tuple[float, float, float]) -> None:
        if self._emitter is not None:
            self._emitter.cone_orientation = _convert_coordinates(cone_orientation)

    def set_cone_inner_angle(self, cone_inner_angle: float) -> None:
        if self._emitter is not None:
            self._cone_inner_angle = int(cone_inner_angle)
            self._set_cone_angles()

    def set_cone_outer_angle(self, cone_outer_angle: float) -> None:
        if self._emitter is not None:
            self._cone_outer_angle = int(cone_outer_angle)
            self._set_cone_angles()

    def _set_cone_angles(self) -> None:
        inner = min(self._cone_inner_angle, self._cone_outer_angle)
        outer = max(self._cone_inner_angle, self._cone_outer_angle)
        self._emitter.set_cone_angles(math.radians(inner), math.radians(outer))

    def set_cone_outer_gain(self, cone_outer_gain: float) -> None:
        if self._emitter is not None:
            self._emitter.cone_outer_volume = cone_outer_gain
