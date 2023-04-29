# vidup

Find duplicated videos.

## Requirements

* Linux
* SSE 4.2 on Intel x86 architecture
    * for crc32
* libsqlite3-dev 3.6.19+
* make
* C++17

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

Convert your video into 16x16 pixels, 12 fps, grayscale raw format:

```sh
$ ffmpeg -loglevel error -i myvideo.mp4 -vf scale=16:16:flags=area -r 12 -an -c:v rawvideo -f rawvideo -pix_fmt gray myvideo.gray
```

The pixel format is hardcoded and cannot be changed.

Register it to database:

```sh
$ vidup myvideo.gray
```

Or, pipe with `vidup --stdin`:

```sh
$ ffmpeg -loglevel error -i myvideo.mp4 -vf scale=16:16:flags=area -r 12 -an -c:v rawvideo -f rawvideo -pix_fmt gray - | vidup --stdin myvideo
```

## Unregister a video

```sh
$ vidup --delete myvideo
```

## Search duplicated videos

List similar videos top ten.

```sh
$ vidup --search myvideo
       4 foo
       1 bar
```

The number on the left is the number of matched scenes.