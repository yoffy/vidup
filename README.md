# vidup

Find duplicated videos.

## Requirements

* Linux
* x86_64 architecture with AVX2
* libsqlite3-dev 3.6.19+
* make
* C++17
* clang-format 14+

## Build

```sh
$ make
```

## Usage

### Initialize the database

```sh
$ vidup --init
```

### Register a video

Convert your video into 16x16 pixels, 30 fps, grayscale raw format:

```sh
$ ffmpeg -loglevel error -i myvideo.mp4 \
  -vf scale=16:16:flags=area \
  -r 30 -an -c:v rawvideo -f rawvideo -pix_fmt gray myvideo.gray
```

The pixel format is hardcoded and cannot be changed. But you can change the frame rate with
`--frame-rate n` (e.g. `vidup --frame-rate 12`).

Register it to database:

```sh
$ vidup myvideo.gray
```

Or, pipe with `vidup --stdin`:

```sh
$ ffmpeg -loglevel error -i myvideo.mp4 \
  -vf scale=16:16:flags=area \
  -r 30 -an -c:v rawvideo -f rawvideo -pix_fmt gray - | vidup --stdin myvideo
```

### Unregister a video

```sh
$ vidup --delete myvideo
```

### Search duplicated videos

List similar videos top ten.

```sh
$ vidup --top
----    123.4 seconds matched
foo
bar
----     56.7 seconds matched
baz
bax
```

List videos similar to `myvideo`:

```sh
$ vidup --search myvideo
       4 foo
       1 bar
```

The number on the left is the number of matched scenes.