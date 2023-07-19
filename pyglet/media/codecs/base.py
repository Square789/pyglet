import io
from typing import TYPE_CHECKING, BinaryIO, List, Optional, Union

from pyglet.media.exceptions import MediaException, CannotSeekException
from pyglet.util import next_or_equal_power_of_two

if TYPE_CHECKING:
    import ctypes
    from pyglet.image import AbstractImage
    from pyglet.image.animation import Animation
    from pyglet.media.codecs import MediaEncoder
    from pyglet.media.drivers.base import MediaEvent
    from pyglet.media.player import Player


class AudioFormat:
    """Audio details.

    An instance of this class is provided by sources with audio tracks.  You
    should not modify the fields, as they are used internally to describe the
    format of data provided by the source.

    Args:
        channels (int): The number of channels: 1 for mono or 2 for stereo
            (pyglet does not yet support surround-sound sources).
        sample_size (int): Bits per sample; only 8 or 16 are supported.
        sample_rate (int): Samples per second (in Hertz).
    """

    def __init__(self, channels: int, sample_size: int, sample_rate: int) -> None:
        self.channels = channels
        self.sample_size = sample_size
        self.sample_rate = sample_rate

        # Convenience

        self.bytes_per_frame = (sample_size // 8) * channels
        self.bytes_per_second = self.bytes_per_frame * sample_rate

        self.bytes_per_sample = self.bytes_per_frame
        """This attribute is kept for compatibility and should not be used.
        For the actual amount of bytes per sample, divide `sample_size` by eight.
        This value contains the bytes per audio frame, and using `bytes_per_frame` should
        be preferred.
        """

    def align(self, num_bytes: int) -> int:
        """Aligns a given amount of bytes to the audio frame size of this format.
        """
        return num_bytes - (num_bytes % self.bytes_per_frame)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, AudioFormat):
            return (self.channels == other.channels and
                    self.sample_size == other.sample_size and
                    self.sample_rate == other.sample_rate)
        return NotImplemented

    def __repr__(self) -> str:
        return '%s(channels=%d, sample_size=%d, sample_rate=%d)' % (
            self.__class__.__name__, self.channels, self.sample_size,
            self.sample_rate)


class VideoFormat:
    """Video details.

    An instance of this class is provided by sources with a video stream. You
    should not modify the fields.

    Note that the sample aspect has no relation to the aspect ratio of the
    video image.  For example, a video image of 640x480 with sample aspect 2.0
    should be displayed at 1280x480.  It is the responsibility of the
    application to perform this scaling.

    Args:
        width (int): Width of video image, in pixels.
        height (int): Height of video image, in pixels.
        sample_aspect (float): Aspect ratio (width over height) of a single
            video pixel.
        frame_rate (float): Frame rate (frames per second) of the video.

            .. versionadded:: 1.2
    """

    def __init__(self, width: int, height: int, sample_aspect: float = 1.0) -> None:
        self.width = width
        self.height = height
        self.sample_aspect = sample_aspect
        self.frame_rate = None

    def __eq__(self, other) -> bool:
        if isinstance(other, VideoFormat):
            return (self.width == other.width and
                    self.height == other.height and
                    self.sample_aspect == other.sample_aspect and
                    self.frame_rate == other.frame_rate)
        return False


class AudioData:
    """A single packet of audio data.

    This class is used internally by pyglet.

    Args:
        data (bytes or ctypes array): Sample data.
        length (int): Size of sample data, in bytes.
        timestamp (float): Time of the first sample, in seconds.
        duration (float): Total data duration, in seconds.
        events (List[:class:`pyglet.media.drivers.base.MediaEvent`]): List of events
            contained within this packet. Events are timestamped relative to
            this audio packet.
    """

    __slots__ = 'data', 'length', 'timestamp', 'duration', 'events'

    def __init__(self,
                 data: Union[bytes, 'ctypes.Array'],
                 length: int,
                 timestamp: float,
                 duration: float,
                 events: List['MediaEvent']) -> None:
        self.data = data
        self.length = length
        self.timestamp = timestamp
        self.duration = duration
        self.events = events

    def __eq__(self, other) -> bool:
        if isinstance(other, AudioData):
            return (self.data == other.data and
                    self.length == other.length and
                    self.timestamp == other.timestamp and
                    self.duration == other.duration and
                    self.events == other.events)
        return NotImplemented

    def consume(self, num_bytes: int, audio_format: AudioFormat) -> None:
        """Remove some data from the beginning of the packet.

        All events are cleared.

        Args:
            num_bytes (int): The number of bytes to consume from the packet.
            audio_format (:class:`.AudioFormat`): The packet audio format.
        """
        self.events = ()
        if num_bytes >= self.length:
            self.data = None
            self.length = 0
            self.timestamp += self.duration
            self.duration = 0.
            return
        elif num_bytes == 0:
            return

        self.data = self.data[num_bytes:]
        self.length -= num_bytes
        self.duration -= num_bytes / audio_format.bytes_per_second
        self.timestamp += num_bytes / audio_format.bytes_per_second

    def get_string_data(self) -> bytes:
        """Return data as a bytestring.

        Returns:
            bytes: Data as a (byte)string.
        """
        if self.data is None:
            return b''

        return memoryview(self.data).tobytes()[:self.length]


class SourceInfo:
    """Source metadata information.

    Fields are the empty string or zero if the information is not available.

    Args:
        title (str): Title
        author (str): Author
        copyright (str): Copyright statement
        comment (str): Comment
        album (str): Album name
        year (int): Year
        track (int): Track number
        genre (str): Genre

    .. versionadded:: 1.2
    """

    title = ''
    author = ''
    copyright = ''
    comment = ''
    album = ''
    year = 0
    track = 0
    genre = ''


class Source:
    """An audio and/or video source.

    Args:
        audio_format (:class:`.AudioFormat`): Format of the audio in this
            source, or ``None`` if the source is silent.
        video_format (:class:`.VideoFormat`): Format of the video in this
            source, or ``None`` if there is no video.
        info (:class:`.SourceInfo`): Source metadata such as title, artist,
            etc; or ``None`` if the` information is not available.

            .. versionadded:: 1.2

    Attributes:
        is_player_source (bool): Determine if this source is a player
            current source.

            Check on a :py:class:`~pyglet.media.player.Player` if this source
            is the current source.
    """

    _players: List['Player'] = []  # Players created through Source.play

    def __init__(self,
                 audio_format: Optional[AudioFormat] = None,
                 video_format: Optional[VideoFormat] = None,
                 info: Optional[SourceInfo] = None) -> None:
        self.audio_format = audio_format
        self.video_format = video_format
        self.info = info

        self._duration = None
        self.is_player_source = False

    @property
    def duration(self) -> float:
        """float: The length of the source, in seconds.

        Not all source durations can be determined; in this case the value
        is ``None``.

        Read-only.
        """
        return self._duration

    def is_precise(self) -> bool:
        """bool: Whether this source is considered precise.

        ``x`` bytes on source ``s`` are considered aligned if
        ``x % s.audio_format.bytes_per_frame == 0``, so there'd be no partial
        audio frame in the returned data.

        A source is precise if - for an aligned request of ``x`` bytes - it
        returns:
          - If ``x`` or more bytes are available, ``x`` bytes.
          - If not enough bytes are available anymore, ``r`` bytes where
            ``r < x`` and ``r`` is aligned.

        A source is **not** precise if it does any of these:
          - Returns less than ``x`` bytes for an aligned request of ``x``
            bytes although data still remains so that an additional request
            would return additional :class:`.AudioData` / not ``None``.
          - Returns more bytes than requested.
          - Returns an unaligned amount of bytes for an aligned request.

        If this method returns ``False``, pyglet will wrap the source in an
        alignment-forcing buffer creating additional overhead.

        If this method is overridden to return ``True``, the step above is
        skipped and pyglet's internals are guaranteed to never make unaligned
        requests, or requests of less than 1024 bytes.

        If this method is overridden to return ``True`` although the source
        does not comply with the requirements above, audio playback may be
        negatively impacted.

        :Returns:
            bool: Whether the source is precise.
        """
        return False

    def acquire(self) -> None:
        """Acquire the source. This is used internally to prevent the same
        source from being queued on multiple player accidentally.
        """
        if self.is_player_source:
            raise MediaException('This source is already queued on a player.')
        self.is_player_source = True

    def release(self) -> None:
        """Release the source to free it up for usage on other players again.
        """
        self.is_player_source = False

    def play(self) -> None:
        """Play the source.

        This is a convenience method which creates a Player for
        this source and plays it immediately.

        Returns:
            :class:`.Player`
        """
        from pyglet.media.player import Player  # XXX Nasty circular dependency
        player = Player()
        player.queue(self)
        player.play()
        Source._players.append(player)

        def _on_player_eos():
            Source._players.remove(player)
            # There is a closure on player. To get the refcount to 0,
            # we need to delete this function.
            player.on_player_eos = None

        player.on_player_eos = _on_player_eos
        return player

    def get_animation(self) -> 'Animation':
        """
        Import all video frames into memory.

        An empty animation will be returned if the source has no video.
        Otherwise, the animation will contain all unplayed video frames (the
        entire source, if it has not been queued on a player). After creating
        the animation, the source will be at EOS (end of stream).

        This method is unsuitable for videos running longer than a
        few seconds.

        .. versionadded:: 1.1

        Returns:
            :class:`pyglet.image.Animation`
        """
        from pyglet.image import Animation, AnimationFrame
        if not self.video_format:
            # XXX: This causes an assertion in the constructor of Animation
            return Animation([])
        else:
            frames = []
            last_ts = 0
            next_ts = self.get_next_video_timestamp()
            while next_ts is not None:
                image = self.get_next_video_frame()
                if image is not None:
                    delay = next_ts - last_ts
                    frames.append(AnimationFrame(image, delay))
                    last_ts = next_ts
                next_ts = self.get_next_video_timestamp()
            return Animation(frames)

    def get_next_video_timestamp(self) -> Optional[float]:
        """Get the timestamp of the next video frame.

        .. versionadded:: 1.1

        Returns:
            float: The next timestamp, or ``None`` if there are no more video
            frames.
        """
        pass

    def get_next_video_frame(self) -> Optional['AbstractImage']:
        """Get the next video frame.

        .. versionadded:: 1.1

        Returns:
            :class:`pyglet.image.AbstractImage`: The next video frame image,
            or ``None`` if the video frame could not be decoded or there are
            no more video frames.
        """
        pass

    def save(self,
             filename: str,
             file: Optional[BinaryIO] = None,
             encoder: Optional['MediaEncoder'] = None) -> None:
        """Save this Source to a file.

        :Parameters:
            `filename` : str
                Used to set the file format, and to open the output file
                if `file` is unspecified.
            `file` : file-like object or None
                File to write audio data to.
            `encoder` : MediaEncoder or None
                If unspecified, all encoders matching the filename extension
                are tried.  If all fail, the exception from the first one
                attempted is raised.

        """
        if encoder:
            return encoder.encode(self, filename, file)
        else:
            import pyglet.media.codecs
            return pyglet.media.codecs.registry.encode(self, filename, file)

    # Internal methods that Player calls on the source:

    def seek(self, timestamp: float) -> None:
        """Seek to given timestamp.

        Args:
            timestamp (float): Time where to seek in the source. The
                ``timestamp`` will be clamped to the duration of the source.
        """
        raise CannotSeekException()

    def get_queue_source(self) -> 'Source':
        """Return the ``Source`` to be used as the queue source for a player.

        Default implementation returns self.
        """
        return self

    def get_audio_data(self, num_bytes: int, compensation_time: float = 0.0) -> Optional[AudioData]:
        """Get next packet of audio data.

        Args:
            num_bytes (int): The requested amount of bytes, returned
                amount may be lower or higher. See the docstring of
                :method:`is_precise` for additional information.
            compensation_time (float): Time in sec to compensate due to a
                difference between the master clock and the audio clock.

        Returns:
            :class:`.AudioData`: Next packet of audio data, or ``None`` if
            there is no (more) data.
        """
        return None


class StreamingSource(Source):
    """A source that is decoded as it is being played.

    The source can only be played once at a time on any
    :class:`~pyglet.media.player.Player`.
    """

    def get_queue_source(self) -> 'StreamingSource':
        """Return the ``Source`` to be used as the source for a player.

        Default implementation returns ``self`` if this source is precise as
        specified by :method:`is_precise`. Otherwise, a new
        :class:`PreciseStreamingSource` wrapping this source is returned.

        Returns:
            :class:`.Source`
        """
        r = self if self.is_precise() else PreciseStreamingSource(self)
        r.acquire()
        return r

    def delete(self) -> None:
        """Release the resources held by this StreamingSource."""
        pass


class StaticSource(Source):
    """A source that has been completely decoded in memory.

    This source can be queued onto multiple players any number of times.

    Construct a :py:class:`~pyglet.media.StaticSource` for the data in
    ``source``.

    Args:
        source (Source):  The source to read and decode audio and video data
            from.
    """

    def __init__(self, source: Source) -> None:
        super().__init__(source.audio_format)

        if source.video_format:
            raise NotImplementedError('Static sources are not supported for video.')

        if not self.audio_format:
            self._data = b""
            self._duration = 0.
            return

        source.acquire()
        # Arbitrary: number of bytes to request at a time.
        buffer_size = 1 << 20  # 1 MB

        # Naive implementation.  Driver-specific implementations may override
        # to load static audio data into device (or at least driver) memory.
        data = io.BytesIO()
        while True:
            audio_data = source.get_audio_data(buffer_size)
            if audio_data is None:
                break
            data.write(audio_data.get_string_data())

        source.release()

        self._data = data.getvalue()
        self._duration = len(self._data) / self.audio_format.bytes_per_second

    def get_queue_source(self) -> 'StaticMemorySource':
        return StaticMemorySource(self._data, self.audio_format)

    def acquire(self) -> None:
        raise RuntimeError('StaticSource cannot be acquired')

    def release(self) -> None:
        raise RuntimeError('StaticSource cannot be released')

    def get_audio_data(self, num_bytes: int, compensation_time: float = 0.0) -> Optional[AudioData]:
        """The StaticSource does not provide audio data.

        When the StaticSource is queued on a
        :class:`~pyglet.media.player.Player`, it creates a
        :class:`.StaticMemorySource` containing its internal audio data and
        audio format.

        Raises:
            RuntimeError
        """
        raise RuntimeError('StaticSource cannot be queued.')


class StaticMemorySource(Source):
    """Helper class for default implementation of :class:`.StaticSource`.

    Do not use directly. This class is used internally by pyglet.

    Args:
        data (bytes or readable buffer): The audio data.
        audio_format (AudioFormat): The audio format.
    """

    def __init__(self, data, audio_format: AudioFormat) -> None:
        """Construct a memory source over the given data buffer."""
        super().__init__(audio_format)

        self._file = io.BytesIO(data)
        self._max_offset = len(data)
        self._duration = len(data) / float(audio_format.bytes_per_second)

    def is_precise(self) -> bool:
        return True

    def get_queue_source(self) -> Source:  # Unsure whether this method should exist
        return StaticMemorySource(self._file.getbuffer(), self.audio_format)

    def seek(self, timestamp: float) -> None:
        """Seek to given timestamp.

        Args:
            timestamp (float): Time where to seek in the source.
        """
        offset = int(timestamp * self.audio_format.bytes_per_second)
        # Align to frame to not return partial ones and corrupt audio data
        self._file.seek(self.audio_format.align(offset))

    def get_audio_data(self, num_bytes: int, compensation_time: float = 0.0) -> Optional[AudioData]:
        """Get next packet of audio data.

        Args:
            num_bytes (int): Maximum number of bytes of data to return.
            compensation_time (float): Not used in this class.

        Returns:
            :class:`.AudioData`: Next packet of audio data, or ``None`` if
            there is no (more) data.
        """
        offset = self._file.tell()
        timestamp = float(offset) / self.audio_format.bytes_per_second

        data = self._file.read(num_bytes)
        if not data:
            return None

        duration = float(len(data)) / self.audio_format.bytes_per_second
        return AudioData(data, len(data), timestamp, duration, [])


class SourceGroup:
    """Group of like sources to allow gapless playback.

    Seamlessly read data from a group of sources to allow for
    gapless playback. All sources must share the same audio format.
    The first source added sets the format.
    """

    def __init__(self) -> None:
        self.audio_format = None
        self.video_format = None
        self.duration = 0.0
        self._timestamp_offset = 0.0
        self._dequeued_durations = []
        self._sources = []

    def seek(self, time: float) -> None:
        if self._sources:
            self._sources[0].seek(time)

    def add(self, source: Source) -> None:
        self.audio_format = self.audio_format or source.audio_format
        source = source.get_queue_source()
        assert (source.audio_format == self.audio_format), "Sources must share the same audio format."
        self._sources.append(source)
        self.duration += source.duration

    def has_next(self) -> bool:
        return len(self._sources) > 1

    def get_queue_source(self) -> 'SourceGroup':
        return self

    def _advance(self) -> None:
        if self._sources:
            self._timestamp_offset += self._sources[0].duration
            self._dequeued_durations.insert(0, self._sources[0].duration)
            old_source = self._sources.pop(0)
            self.duration -= old_source.duration

            if isinstance(old_source, StreamingSource):
                old_source.delete()
                del old_source

    def get_audio_data(self, num_bytes: int, compensation_time: float = 0.0) -> Optional[AudioData]:
        """Get next audio packet.

        :Parameters:
            `num_bytes` : int
                Hint for preferred size of audio packet; may be ignored.

        :rtype: `AudioData`
        :return: Audio data, or None if there is no more data.
        """

        if not self._sources:
            return None

        buffer = b""
        duration = 0.0
        timestamp = 0.0

        while len(buffer) < num_bytes and self._sources:
            audiodata = self._sources[0].get_audio_data(num_bytes)
            if audiodata:
                buffer += audiodata.data
                duration += audiodata.duration
                timestamp += self._timestamp_offset
            else:
                self._advance()

        return AudioData(buffer, len(buffer), timestamp, duration, [])


class PreciseStreamingSource(StreamingSource):
    """Wrap non-precise sources that may over- or undershoot.

    Purpose of this source is to always return data whose length is equal or
    less than in length, where less hints at definite source exhaustion.

    This source is used by pyglet internally, you probably don't need to
    bother with it.

    This source erases AudioData-contained timestamp/duration information and
    events.
    """

    def __init__(self, source: Source) -> None:
        super().__init__(source.audio_format, source.video_format, source.info)

        self._source = source
        self._buffer = bytearray()
        self._exhausted = False

        # Forward duration
        self._duration = source.duration

    def is_precise(self) -> bool:
        return True

    # And forward other operations to the underlying source too
    def acquire(self) -> None:
        super().acquire()
        self._source.acquire()

    def release(self) -> None:
        super().release()
        self._source.release()

    def seek(self, timestamp: float) -> None:
        self._buffer.clear()
        self._exhausted = False
        self._source.seek(timestamp)

    def get_audio_data(self, num_bytes: int, _compensation_time: float = 0.0) -> Optional[AudioData]:
        if self._exhausted:
            return None

        if len(self._buffer) < num_bytes:
            # Buffer is incapable of fulfilling request, get more

            # Reduce amount of required bytes by buffer length
            required_bytes = num_bytes - len(self._buffer)

            # Don't bother with super-small requests to something that likely does some form of I/O
            # Also, intentionally overshoot since some sources may just barely undercut.
            attempt = next_or_equal_power_of_two(max(4096, required_bytes + 16))
            for attempt_size in (attempt, attempt * 2, attempt * 8):
                res = self._source.get_audio_data(attempt_size)
                if res is None:
                    self._exhausted = True
                    break

                self._buffer += res.data
                if len(self._buffer) >= num_bytes:
                    # Got enough
                    break
            else:
                # We didn't receive None, but also didn't get enough after requesting more than 10
                # times of the needed amount. Fake exhaustion in this case.
                self._exhausted = True

        res = bytes(self._buffer[:num_bytes])
        del self._buffer[:num_bytes]
        return AudioData(res, len(res), -1.0, -1.0, [])
