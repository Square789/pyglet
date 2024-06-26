"""Display positioned, scaled and rotated images.

A sprite is an instance of an image displayed on-screen.  Multiple sprites can
display the same image at different positions on the screen.  Sprites can also
be scaled larger or smaller, rotated at any angle and drawn at a fractional
opacity.

The following complete example loads a ``"ball.png"`` image and creates a
sprite for that image.  The sprite is then drawn in the window's
draw event handler::

    import pyglet

    ball_image = pyglet.image.load('ball.png')
    ball = pyglet.sprite.Sprite(ball_image, x=50, y=50)

    window = pyglet.window.Window()

    @window.event
    def on_draw():
        ball.draw()

    pyglet.app.run()

The sprite can be moved by modifying the :py:attr:`~pyglet.sprite.Sprite.x` and 
:py:attr:`~pyglet.sprite.Sprite.y` properties.  Other
properties determine the sprite's :py:attr:`~pyglet.sprite.Sprite.rotation`,
:py:attr:`~pyglet.sprite.Sprite.scale` and
:py:attr:`~pyglet.sprite.Sprite.opacity`.

By default, sprite coordinates are restricted to integer values to avoid
sub-pixel artifacts.  If you require to use floats, for example for smoother
animations, you can set the ``subpixel`` parameter to ``True`` when creating
the sprite (:since: pyglet 1.2).

The sprite's positioning, rotation and scaling all honor the original
image's anchor (:py:attr:`~pyglet.image.AbstractImage.anchor_x`,
:py:attr:`~pyglet.image.AbstractImage.anchor_y`).


Drawing multiple sprites
========================

Sprites can be "batched" together and drawn at once more quickly than if each
of their ``draw`` methods were called individually.  The following example
creates one hundred ball sprites and adds each of them to a :py:class:`~pyglet.graphics.Batch`.  The
entire batch of sprites is then drawn in one call::

    batch = pyglet.graphics.Batch()

    ball_sprites = []
    for i in range(100):
        x, y = i * 10, 50
        ball_sprites.append(pyglet.sprite.Sprite(ball_image, x, y, batch=batch))

    @window.event
    def on_draw():
        batch.draw()

Sprites can be freely modified in any way even after being added to a batch,
however a sprite can belong to at most one batch.  See the documentation for
:py:mod:`pyglet.graphics` for more details on batched rendering, and grouping of
sprites within batches.

.. versionadded:: 1.1
"""
from __future__ import annotations

import sys

from typing import TYPE_CHECKING

import pyglet

from pyglet.gl import *
from pyglet import clock
from pyglet import event
from pyglet import graphics
from pyglet import image

_is_pyglet_doc_run = hasattr(sys, "is_pyglet_doc_run") and sys.is_pyglet_doc_run

if TYPE_CHECKING:
    from pyglet.image import AbstractImage, Texture, Animation
    from pyglet.graphics import Batch, Group
    from pyglet.graphics.shader import ShaderProgram


vertex_source: str = """#version 150 core
    in vec3 translate;
    in vec4 colors;
    in vec3 tex_coords;
    in vec2 scale;
    in vec3 position;
    in float rotation;

    out vec4 vertex_colors;
    out vec3 texture_coords;

    uniform WindowBlock
    {
        mat4 projection;
        mat4 view;
    } window;

    mat4 m_scale = mat4(1.0);
    mat4 m_rotation = mat4(1.0);
    mat4 m_translate = mat4(1.0);

    void main()
    {
        m_scale[0][0] = scale.x;
        m_scale[1][1] = scale.y;
        m_translate[3][0] = translate.x;
        m_translate[3][1] = translate.y;
        m_translate[3][2] = translate.z;
        m_rotation[0][0] =  cos(-radians(rotation)); 
        m_rotation[0][1] =  sin(-radians(rotation));
        m_rotation[1][0] = -sin(-radians(rotation));
        m_rotation[1][1] =  cos(-radians(rotation));

        gl_Position = window.projection * window.view * m_translate * m_rotation * m_scale * vec4(position, 1.0);

        vertex_colors = colors;
        texture_coords = tex_coords;
    }
"""

fragment_source: str = """#version 150 core
    in vec4 vertex_colors;
    in vec3 texture_coords;
    out vec4 final_colors;

    uniform sampler2D sprite_texture;

    void main()
    {
        final_colors = texture(sprite_texture, texture_coords.xy) * vertex_colors;
    }
"""

fragment_array_source: str = """#version 150 core
    in vec4 vertex_colors;
    in vec3 texture_coords;
    out vec4 final_colors;

    uniform sampler2DArray sprite_texture;

    void main()
    {
        final_colors = texture(sprite_texture, texture_coords) * vertex_colors;
    }
"""


def get_default_shader() -> ShaderProgram:
    return pyglet.gl.current_context.create_program((vertex_source, 'vertex'),
                                                    (fragment_source, 'fragment'))


def get_default_array_shader() -> ShaderProgram:
    return pyglet.gl.current_context.create_program((vertex_source, 'vertex'),
                                                    (fragment_array_source, 'fragment'))


class SpriteGroup(graphics.Group):
    """Shared Sprite rendering Group.

    The Group defines custom ``__eq__`` ane ``__hash__`` methods, and so will
    be automatically coalesced with other Sprite Groups sharing the same parent
    Group, Texture and blend parameters.
    """

    def __init__(self, texture: Texture, blend_src: int, blend_dest: int,
                 program: ShaderProgram, parent: Group | None = None):
        """Create a sprite group.

        The group is created internally when a :py:class:`~pyglet.sprite.Sprite`
        is created; applications usually do not need to explicitly create it.

        Args:
            texture:
                The (top-level) texture containing the sprite image.
            blend_src:
                OpenGL blend source mode; for example,
                ``GL_SRC_ALPHA``.
            blend_dest:
                OpenGL blend destination mode; for example,
                ``GL_ONE_MINUS_SRC_ALPHA``.
            program:
                A custom ShaderProgram.
            parent:
                Optional parent group.
        """
        super().__init__(parent=parent)
        self.texture = texture
        self.blend_src = blend_src
        self.blend_dest = blend_dest
        self.program = program

    def set_state(self) -> None:
        self.program.use()

        glActiveTexture(GL_TEXTURE0)
        glBindTexture(self.texture.target, self.texture.id)

        glEnable(GL_BLEND)
        glBlendFunc(self.blend_src, self.blend_dest)

    def unset_state(self) -> None:
        glDisable(GL_BLEND)
        self.program.stop()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.texture})"

    def __eq__(self, other: SpriteGroup) -> bool:
        return (other.__class__ is self.__class__ and
                self.program is other.program and
                self.parent == other.parent and
                self.texture.target == other.texture.target and
                self.texture.id == other.texture.id and
                self.blend_src == other.blend_src and
                self.blend_dest == other.blend_dest)

    def __hash__(self) -> int:
        return hash((self.program, self.parent,
                     self.texture.id, self.texture.target,
                     self.blend_src, self.blend_dest))


class Sprite(event.EventDispatcher):
    """Presend and manipulate an on-screen image.

    See the module documentation for usage.
    """

    _batch = None
    _animation = None
    _frame_index = 0
    _paused = False
    _rotation = 0
    _opacity = 255
    _rgb: tuple[int, int, int] = (255, 255, 255)
    _scale = 1.0
    _scale_x = 1.0
    _scale_y = 1.0
    _visible = True
    _vertex_list = None
    group_class: Group = SpriteGroup

    def __init__(self,
                 img: AbstractImage | Animation,
                 x: float = 0, y: float = 0, z: float = 0,
                 blend_src: int = GL_SRC_ALPHA,
                 blend_dest: int = GL_ONE_MINUS_SRC_ALPHA,
                 batch: Batch | None = None,
                 group: Group | None = None,
                 subpixel: bool = False):
        """Create a Sprite instance.

        Args:
            img:
                Image or Animation to display.
            x:
                X coordinate of the sprite.
            y:
                Y coordinate of the sprite.
            z:
                Z coordinate of the sprite.
            blend_src:
                OpenGL blend source mode.  The default is suitable for
                compositing sprites drawn from back-to-front.
            blend_dest:
                OpenGL blend destination mode.  The default is suitable for
                compositing sprites drawn from back-to-front.
            batch:
                Optional batch to add the sprite to.
            group:
                Optional parent group of the sprite.
            subpixel:
                Allow floating-point coordinates for the sprite. By default,
                coordinates are restricted to integer values.
        """
        self._x = x
        self._y = y
        self._z = z
        self._img = img

        if isinstance(img, image.Animation):
            self._animation = img
            self._texture = img.frames[0].image.get_texture()
            self._next_dt = img.frames[0].duration
            if self._next_dt:
                clock.schedule_once(self._animate, self._next_dt)
        else:
            self._texture = img.get_texture()

        self._batch = batch
        self._group = self.group_class(self._texture, blend_src, blend_dest, self.program, group)
        self._subpixel = subpixel
        self._create_vertex_list()

    @property
    def program(self) -> ShaderProgram:
        if isinstance(self._img, image.TextureArrayRegion):
            program = get_default_array_shader()
        else:
            program = get_default_shader()

        return program

    def __del__(self):
        try:
            if self._vertex_list is not None:
                self._vertex_list.delete()
        except:
            pass

    def delete(self) -> None:
        """Force immediate removal of the sprite from video memory.

        It is recommended to call this whenever you delete a sprite,
        as the Python garbage collector will not necessarily call the
        finalizer as soon as the sprite falls out of scope.
        """
        if self._animation:
            clock.unschedule(self._animate)
        self._vertex_list.delete()
        self._vertex_list = None
        self._texture = None

        # Easy way to break circular reference, speeds up GC
        self._group = None

    def _animate(self, dt: float) -> None:
        self._frame_index += 1
        if self._frame_index >= len(self._animation.frames):
            self._frame_index = 0
            self.dispatch_event('on_animation_end')
            if self._vertex_list is None:
                return  # Deleted in event handler.

        frame = self._animation.frames[self._frame_index]
        self._set_texture(frame.image.get_texture())

        if frame.duration is not None:
            duration = frame.duration - (self._next_dt - dt)
            duration = min(max(0, duration), frame.duration)
            clock.schedule_once(self._animate, duration)
            self._next_dt = duration
        else:
            self.dispatch_event('on_animation_end')

    @property
    def batch(self) -> Batch:
        """Graphics batch.

        The sprite can be migrated from one batch to another, or removed from
        its batch (for individual drawing).  Note that this can be an expensive
        operation.
        """
        return self._batch

    @batch.setter
    def batch(self, batch: Batch) -> None:
        if self._batch == batch:
            return

        if batch is not None and self._batch is not None:
            self._batch.migrate(self._vertex_list, GL_TRIANGLES, self._group, batch)
            self._batch = batch
        else:
            self._vertex_list.delete()
            self._batch = batch
            self._create_vertex_list()

    @property
    def group(self) -> Group:
        """Parent graphics group.

        The Sprite can change its rendering group, however this
        can be a relatively expensive operation.
        """
        return self._group.parent

    @group.setter
    def group(self, group):
        if self._group.parent == group:
            return
        self._group = self.group_class(self._texture,
                                       self._group.blend_src,
                                       self._group.blend_dest,
                                       self._group.program,
                                       group)
        if self._batch is not None:
            self._batch.migrate(self._vertex_list, GL_TRIANGLES, self._group, self._batch)

    @property
    def image(self) -> AbstractImage | Animation:
        """The Sprite's Image or Animation to display."""
        if self._animation:
            return self._animation
        return self._texture

    @image.setter
    def image(self, img: AbstractImage | Animation) -> None:
        if self._animation is not None:
            clock.unschedule(self._animate)
            self._animation = None

        if isinstance(img, image.Animation):
            self._animation = img
            self._frame_index = 0
            self._set_texture(img.frames[0].image.get_texture())
            self._next_dt = img.frames[0].duration
            if self._next_dt:
                clock.schedule_once(self._animate, self._next_dt)
        else:
            self._set_texture(img.get_texture())
        self._update_position()

    def _set_texture(self, texture: Texture) -> None:
        if texture.id is not self._texture.id:
            self._group = self._group.__class__(texture,
                                                self._group.blend_src,
                                                self._group.blend_dest,
                                                self._group.program,
                                                self._group.parent)
            self._vertex_list.delete()
            self._texture = texture
            self._create_vertex_list()
        else:
            self._vertex_list.tex_coords[:] = texture.tex_coords
        self._texture = texture

    def _create_vertex_list(self) -> None:
        self._vertex_list = self.program.vertex_list_indexed(
            4, GL_TRIANGLES, [0, 1, 2, 0, 2, 3], self._batch, self._group,
            position=('f', self._get_vertices()),
            colors=('Bn', (*self._rgb, int(self._opacity)) * 4),
            translate=('f', (self._x, self._y, self._z) * 4),
            scale=('f', (self._scale*self._scale_x, self._scale*self._scale_y) * 4),
            rotation=('f', (self._rotation,) * 4),
            tex_coords=('f', self._texture.tex_coords))

    def _get_vertices(self) -> tuple:
        if not self._visible:
            return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        else:
            img = self._texture
            x1 = -img.anchor_x
            y1 = -img.anchor_y
            x2 = x1 + img.width
            y2 = y1 + img.height
            vertices = (x1, y1, 0, x2, y1, 0, x2, y2, 0, x1, y2, 0)

            if not self._subpixel:
                return tuple(map(int, vertices))
            else:
                return vertices

    def _update_position(self) -> None:
        self._vertex_list.position[:] = self._get_vertices()

    @property
    def position(self) -> tuple[float, float, float]:
        """The (x, y, z) coordinates of the sprite, as a tuple."""
        return self._x, self._y, self._z

    @position.setter
    def position(self, position: tuple[float, float, float]) -> None:
        self._x, self._y, self._z = position
        self._vertex_list.translate[:] = position * 4

    @property
    def x(self) -> float:
        """X coordinate of the sprite."""
        return self._x

    @x.setter
    def x(self, x):
        self._x = x
        self._vertex_list.translate[:] = (x, self._y, self._z) * 4

    @property
    def y(self) -> float:
        """Y coordinate of the sprite."""
        return self._y

    @y.setter
    def y(self, y):
        self._y = y
        self._vertex_list.translate[:] = (self._x, y, self._z) * 4

    @property
    def z(self) -> float:
        """Z coordinate of the sprite."""
        return self._z

    @z.setter
    def z(self, z):
        self._z = z
        self._vertex_list.translate[:] = (self._x, self._y, z) * 4

    @property
    def rotation(self) -> float:
        """Clockwise rotation of the sprite, in degrees.

        The sprite image will be rotated about its image's (anchor_x, anchor_y)
        position.
        """
        return self._rotation

    @rotation.setter
    def rotation(self, rotation: float):
        self._rotation = rotation
        self._vertex_list.rotation[:] = (self._rotation,) * 4

    @property
    def scale(self) -> float:
        """Base Scaling factor.

        A scaling factor of 1.0 (the default) has no effect. A scale of
        2.0 will draw the sprite at twice the native size of its image.
        """
        return self._scale

    @scale.setter
    def scale(self, scale):
        self._scale = scale
        self._vertex_list.scale[:] = (scale * self._scale_x, scale * self._scale_y) * 4

    @property
    def scale_x(self) -> float:
        """Horizontal scaling factor.

         A scaling factor of 1.0 (the default) has no effect. A scale of
         2.0 will draw the sprite at twice the native width of its image.
        """
        return self._scale_x

    @scale_x.setter
    def scale_x(self, scale_x):
        self._scale_x = scale_x
        self._vertex_list.scale[:] = (self._scale * scale_x, self._scale * self._scale_y) * 4

    @property
    def scale_y(self):
        """Vertical scaling factor.

         A scaling factor of 1.0 (the default) has no effect. A scale of
         2.0 will draw the sprite at twice the native height of its image.
        """
        return self._scale_y

    @scale_y.setter
    def scale_y(self, scale_y):
        self._scale_y = scale_y
        self._vertex_list.scale[:] = (self._scale * self._scale_x, self._scale * scale_y) * 4

    def update(self, x: float | None = None, y: float | None = None, z: float | None = None,
               rotation: float | None = None, scale: float | None = None,
               scale_x: float | None = None, scale_y: float | None = None):
        """Simultaneously change the position, rotation or scale.

        This method is provided for convenience. There is not much
        performance benefit to updating multiple Sprite attributes at once.

        Args:
            x:
                X coordinate of the sprite.
            y:
                Y coordinate of the sprite.
            z:
                Z coordinate of the sprite.
            rotation:
                Clockwise rotation of the sprite, in degrees.
            scale:
                Scaling factor.
            scale_x:
                Horizontal scaling factor.
            scale_y:
                Vertical scaling factor.
        """

        translations_outdated = False

        # only bother updating if the translation actually changed
        if x is not None:
            self._x = x
            translations_outdated = True
        if y is not None:
            self._y = y
            translations_outdated = True
        if z is not None:
            self._z = z
            translations_outdated = True

        if translations_outdated:
            self._vertex_list.translate[:] = (self._x, self._y, self._z) * 4

        if rotation is not None and rotation != self._rotation:
            self._rotation = rotation
            self._vertex_list.rotation[:] = (rotation,) * 4

        scales_outdated = False

        # only bother updating if the scale actually changed
        if scale is not None:
            self._scale = scale
            scales_outdated = True
        if scale_x is not None:
            self._scale_x = scale_x
            scales_outdated = True
        if scale_y is not None:
            self._scale_y = scale_y
            scales_outdated = True

        if scales_outdated:
            self._vertex_list.scale[:] = (self._scale * self._scale_x, self._scale * self._scale_y) * 4

    @property
    def width(self) -> float:
        """Scaled width of the sprite.

        Invariant under rotation.
        """
        w = self._texture.width * abs(self._scale_x) * abs(self._scale)
        return w if self._subpixel else int(w)

    @width.setter
    def width(self, width: float):
        self.scale_x = width / (self._texture.width * abs(self._scale))

    @property
    def height(self) -> float:
        """Scaled height of the sprite.

        Invariant under rotation.
        """
        h = self._texture.height * abs(self._scale_y) * abs(self._scale)
        return h if self._subpixel else int(h)

    @height.setter
    def height(self, height: float):
        self.scale_y = height / (self._texture.height * abs(self._scale))

    @property
    def opacity(self) -> int:
        """Blend opacity.

        This property sets the alpha component of the colour of the sprite's
        vertices.  With the default blend mode (see the constructor), this
        allows the sprite to be drawn with fractional opacity, blending with the
        background.

        An opacity of 255 (the default) has no effect.  An opacity of 128 will
        make the sprite appear translucent.
        """
        return self._opacity

    @opacity.setter
    def opacity(self, opacity: int):
        self._opacity = opacity
        self._vertex_list.colors[:] = (*self._rgb, int(self._opacity)) * 4

    @property
    def color(self) -> tuple[int, int, int]:
        """Blend color.

        This property sets the color of the sprite's vertices. This allows the
        sprite to be drawn with a color tint.

        The color is specified as an RGB tuple of integers '(red, green, blue)'.
        Each color component must be in the range 0 (dark) to 255 (saturated).
        """
        return self._rgb

    @color.setter
    def color(self, rgb: tuple[int, int, int]):
        self._rgb = int(rgb[0]), int(rgb[1]), int(rgb[2])
        self._vertex_list.colors[:] = (*self._rgb, int(self._opacity)) * 4

    @property
    def visible(self) -> bool:
        """True if the sprite will be drawn."""
        return self._visible

    @visible.setter
    def visible(self, visible):
        self._visible = visible
        self._update_position()

    @property
    def paused(self) -> bool:
        """Pause/resume the Sprite's Animation

        If ``Sprite.image`` is an Animation, you can pause or resume
        the animation by setting this property to True or False.
        If not an Animation, this has no effect.
        """
        return self._paused

    @paused.setter
    def paused(self, pause):
        if not hasattr(self, '_animation') or pause == self._paused:
            return
        if pause is True:
            clock.unschedule(self._animate)
        else:
            frame = self._animation.frames[self._frame_index]
            self._next_dt = frame.duration
            if self._next_dt:
                clock.schedule_once(self._animate, self._next_dt)
        self._paused = pause

    @property
    def frame_index(self) -> int:
        """The current Animation frame.

        If the ``Sprite.image`` is an ``Animation``, you can query or set
        the current frame. If not an Animation, this will always be 0.
        """
        return self._frame_index

    @frame_index.setter
    def frame_index(self, index: int):
        # Bound to available number of frames
        if self._animation is None:
            return
        self._frame_index = max(0, min(index, len(self._animation.frames)-1))

    def draw(self) -> None:
        """Draw the sprite at its current position.

        See the module documentation for hints on drawing multiple sprites
        efficiently.
        """
        self._group.set_state_recursive()
        self._vertex_list.draw(GL_TRIANGLES)
        self._group.unset_state_recursive()

    if _is_pyglet_doc_run:
        # Events

        def on_animation_end(self):
            """The sprite animation reached the final frame.

            The event is triggered only if the sprite has an animation, not an
            image.  For looping animations, the event is triggered each time
            the animation loops.
            """


Sprite.register_event_type('on_animation_end')


class AdvancedSprite(pyglet.sprite.Sprite):
    """Is a sprite that lets you change the shader program during initialization and after
    For advanced users who understand shaders."""
    def __init__(self,
                 img, x=0, y=0, z=0,
                 blend_src=GL_SRC_ALPHA,
                 blend_dest=GL_ONE_MINUS_SRC_ALPHA,
                 batch=None,
                 group=None,
                 subpixel=False,
                 program=None):

        self._program = program

        if not program:
            if isinstance(img, image.TextureArrayRegion):
                self._program = get_default_array_shader()
            else:
                self._program = get_default_shader()

        super().__init__(img, x, y, z, blend_src, blend_dest, batch, group, subpixel)

    @property
    def program(self) -> ShaderProgram:
        return self._program

    @program.setter
    def program(self, program: ShaderProgram):
        if self._program == program:
            return
        self._group = self.group_class(self._texture,
                                       self._group.blend_src,
                                       self._group.blend_dest,
                                       program,
                                       self._group)
        self._batch.migrate(self._vertex_list, GL_TRIANGLES, self._group, self._batch)
        self._program = program




