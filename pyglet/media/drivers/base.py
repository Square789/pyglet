from collections import deque
import math
import weakref
from abc import ABCMeta, abstractmethod

import pyglet
from pyglet.util import debug_print


_debug = debug_print('debug_media')


class AbstractAudioPlayer(metaclass=ABCMeta):
    """Base class for driver audio players to be used by the high-level
    player. Relies on a thread to regularly call a `work` method in order
    for it to operate.
    """

    # Audio synchronization constants
    AUDIO_DIFF_AVG_NB = 20
    # no audio correction is done if too big error
    AV_NOSYNC_THRESHOLD = 10.0

    def __init__(self, source, player):
        """Create a new audio player.

        :Parameters:
            `source` : `Source`
                Source to play from.
            `player` : `Player`
                Player to receive EOS and video frame sync events.

        """
        # We only keep weakref to the player and its source to avoid
        # circular references. It's the player who owns the source and
        # the audio_player
        self.source = weakref.proxy(source)
        self.player = weakref.proxy(player)

        fmt = source.audio_format
        # For drivers that do not operate on buffer submission, but write calls
        # into what is effectively a single buffer exposed to pyglet
        self._singlebuffer_ideal_size = max(32768, fmt.align(int(fmt.bytes_per_second * 1.0)))

        # At which point a driver should try and refill data from the source
        self._buffered_data_comfortable_limit = int(self._singlebuffer_ideal_size * (2/3))

        # For drivers that operate on buffer submission
        self._ideal_buffer_size = fmt.align(int(fmt.bytes_per_second * 0.333333))
        self._ideal_queued_buffer_count = 3

        # A deque of (play_cursor, MediaEvent)
        self._events = deque()

        # Audio synchronization
        self.audio_diff_avg_count = 0
        self.audio_diff_cum = 0.0
        self.audio_diff_avg_coef = math.exp(math.log10(0.01) / self.AUDIO_DIFF_AVG_NB)
        self.audio_diff_threshold = 0.1  # Experimental. ffplay computes it differently

    def on_driver_destroy(self):
        """Called before the audio driver is going to be destroyed (a planned destroy)."""
        pass

    def on_driver_reset(self):
        """Called after the audio driver has been re-initialized."""
        pass

    def set_source(self, source):
        """Change the player's source for a new one.
        It must be of the same audio format.
        Will clear the player, make sure you paused it beforehand.
        """
        assert self.source.audio_format == source.audio_format

        self.clear()
        self.source = weakref.proxy(source)

    @abstractmethod
    def prefill_audio(self):
        """Prefill the audio buffer with audio data.

        This method is called before the audio player starts in order to
        have it play as soon as possible.
        """
        # It is illegal to call this method while the player is playing.

    @abstractmethod
    def work(self):
        """Ran regularly by the worker thread. This method should fill up
        the player's buffers if required, and dispatch any necessary events.
        """
        # Implementing this method in tandem with the others safely is prone to pitfalls,
        # so here's some hints:
        # It may get called from the worker thread. While that happens, the worker thread
        # will hold its operation lock.
        # These things may happen to the player while `work` is being called:
        #
        # It may get paused/unpaused.
        # - Receiving data after getting paused/unpaused is typically not a problem.
        #   Realistically, this won't happen as all implementations include a
        #   `self.driver.worker.remove/add(self)` snippet in their `play`/`pause`
        #   implementations.
        #
        # It may *not* get cleared.
        # - It is illegal to call clear on an unpaused player, and only playing players
        #   are in the worker thread.
        #
        # It may get deleted.
        # - In this case, attempting to continue with the data will cause some sort of error
        #   To combat this, all `delete` implementations contain a form of
        #   `self.driver.worker.remove(self)`.
        # This method may also be called from the main thread through `prefill_audio`.
        # This will only happen before the player is started: `work` will never interfere
        # with itself.
        # Audioplayers are not threadsafe and should only be interacted with through the main
        # thread.
        # TODO: That last line is more directed to pyglet users, maybe throw it into the docs
        # somewhere.
        pass

    @abstractmethod
    def play(self):
        """Begin playback."""

    @abstractmethod
    def stop(self):
        """Stop (pause) playback."""

    @abstractmethod
    def clear(self):
        """Clear all buffered data and prepare for replacement data.

        The player must be stopped before calling this method.
        """
        self._events.clear()
        self.audio_diff_avg_count = 0
        self.audio_diff_cum = 0.0

    @abstractmethod
    def delete(self):
        """Stop playing and clean up all resources used by player."""
        # This may be called from high level Players on shutdown after the player's driver
        # has been deleted. AudioPlayer implementations must handle this.

    @abstractmethod
    def get_time(self):
        """Return approximation of current playback time within current source.

        Returns ``None`` if the audio player does not know what the playback
        time is (for example, before any valid audio data has been read).

        :rtype: float
        :return: current play cursor time, in seconds.
        """
        # TODO determine which source within group

    def _play_group(self, audio_players):
        """Begin simultaneous playback on a list of audio players."""
        # This should be overridden by subclasses for better synchrony.
        for player in audio_players:
            player.play()

    def _stop_group(self, audio_players):
        """Stop simultaneous playback on a list of audio players."""
        # This should be overridden by subclasses for better synchrony.
        for player in audio_players:
            player.stop()

    def append_events(self, start_index, events):
        """Append the given :class:`MediaEvent`s to the events deque using
        the current source's audio format and the supplied ``start_index``
        to convert their timestamps to dispatch indices.
        """
        bps = self.source.audio_format.bytes_per_second
        for event in events:
            event_cursor = start_index + event.timestamp * bps
            assert _debug(f'AbstractAudioPlayer: Adding event {event} at {event_cursor}')
            self._events.append((event_cursor, event))

    def dispatch_media_events(self, until_index):
        """Dispatch all :class:`MediaEvent`s whose index is less than or equal
        to the specified ``until_index``.
        """
        while self._events and self._events[0][0] <= until_index:
            self._events.popleft()[1].sync_dispatch_to_player(self.player)

    def get_audio_time_diff(self):
        """Queries the time difference between the audio time and the `Player`
        master clock.

        The time difference returned is calculated using a weighted average on
        previous audio time differences. The algorithms will need at least 20
        measurements before returning a weighted average.

        :rtype: float
        :return: weighted average difference between audio time and master
            clock from `Player`
        """
        audio_time = self.get_time() or 0
        p_time = self.player.time
        diff = audio_time - p_time
        if abs(diff) < self.AV_NOSYNC_THRESHOLD:
            self.audio_diff_cum = diff + self.audio_diff_cum * self.audio_diff_avg_coef
            if self.audio_diff_avg_count < self.AUDIO_DIFF_AVG_NB:
                self.audio_diff_avg_count += 1
            else:
                avg_diff = self.audio_diff_cum * (1 - self.audio_diff_avg_coef)
                if abs(avg_diff) > self.audio_diff_threshold:
                    return avg_diff
        else:
            self.audio_diff_avg_count = 0
            self.audio_diff_cum = 0.0
        return 0.0

    def set_volume(self, volume):
        """See `Player.volume`."""
        pass

    def set_position(self, position):
        """See :py:attr:`~pyglet.media.Player.position`."""
        pass

    def set_min_distance(self, min_distance):
        """See `Player.min_distance`."""
        pass

    def set_max_distance(self, max_distance):
        """See `Player.max_distance`."""
        pass

    def set_pitch(self, pitch):
        """See :py:attr:`~pyglet.media.Player.pitch`."""
        pass

    def set_cone_orientation(self, cone_orientation):
        """See `Player.cone_orientation`."""
        pass

    def set_cone_inner_angle(self, cone_inner_angle):
        """See `Player.cone_inner_angle`."""
        pass

    def set_cone_outer_angle(self, cone_outer_angle):
        """See `Player.cone_outer_angle`."""
        pass

    def set_cone_outer_gain(self, cone_outer_gain):
        """See `Player.cone_outer_gain`."""
        pass


AbstractAudioPlayer = AbstractAudioPlayer


class AbstractAudioDriver(metaclass=ABCMeta):
    @abstractmethod
    def create_audio_player(self, source, player):
        pass

    @abstractmethod
    def get_listener(self):
        pass

    @abstractmethod
    def delete(self):
        pass


class MediaEvent:
    """Representation of a media event.

    These events are used internally by some audio driver implementation to
    communicate events to the :class:`~pyglet.media.player.Player`.
    One example is the ``on_eos`` event.

    Args:
        event (str): Event description.
        timestamp (float): The time when this event happens.
        *args: Any required positional argument to go along with this event.
    """

    __slots__ = 'event', 'timestamp', 'args'

    def __init__(self, event, timestamp=0.0, *args):
        # Meaning of timestamp is dependent on context; and not seen by application.
        self.event = event
        self.timestamp = timestamp
        self.args = args

    def sync_dispatch_to_player(self, player):
        pyglet.app.platform_event_loop.post_event(player, self.event, *self.args)

    def __repr__(self):
        return f"MediaEvent({self.event}, {self.timestamp}, {self.args})"

    def __lt__(self, other):
        if not isinstance(other, MediaEvent):
            return NotImplemented
        return self.timestamp < other.timestamp
