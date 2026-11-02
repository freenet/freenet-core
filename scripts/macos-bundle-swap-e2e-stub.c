/*
 * Test stub for scripts/macos-bundle-swap-e2e.sh. Compiled at CI time
 * via clang and dropped into each test .app bundle as Contents/MacOS/
 * freenet-bin.
 *
 * Rationale: the updater script (crates/core/scripts/macos-bundle-updater.sh) uses
 * `pgrep -f "^${bundle}/Contents/MacOS/"` to detect the running Freenet
 * process. That pattern assumes the kernel invoked a real Mach-O at the
 * bundle path, so the process command line begins with that path. A
 * shell-script freenet-bin does not satisfy this: the shebang hands off
 * to /bin/bash and the process command line starts with `/bin/bash`
 * instead, causing pgrep to miss the process and the wait-for-exit
 * loop to spin uselessly. Compiling a real binary restores the
 * pgrep-observable shape that production uses.
 *
 * Usage:
 *   freenet-bin <marker_file> <version> <lifetime_seconds>
 *   freenet-bin
 *
 * Writes <version> to <marker_file> on startup (so the test can assert
 * which bundle is actually running) then sleeps for <lifetime_seconds>
 * and exits. With no arguments, which is how LaunchServices starts a
 * bundle's executable, it reads the three values, one per line, from
 * Contents/Resources/stub-args in its own bundle.
 */

#include <libgen.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* dirname(3) may modify its argument, so hand it a copy. */
static const char *dirname_of(const char *path) {
    static char copy[PATH_MAX];
    snprintf(copy, sizeof copy, "%s", path);
    return dirname(copy);
}

static int read_line(FILE *f, char *out, size_t size) {
    if (!fgets(out, (int)size, f)) {
        return 0;
    }
    out[strcspn(out, "\n")] = '\0';
    return 1;
}

int main(int argc, char *argv[]) {
    char marker[PATH_MAX];
    char version[64];
    char lifetime_text[32];

    if (argc == 4) {
        snprintf(marker, sizeof marker, "%s", argv[1]);
        snprintf(version, sizeof version, "%s", argv[2]);
        snprintf(lifetime_text, sizeof lifetime_text, "%s", argv[3]);
    } else if (argc == 1) {
        char path[PATH_MAX];
        snprintf(path, sizeof path, "%s/../Resources/stub-args",
                 dirname_of(argv[0]));
        FILE *args = fopen(path, "r");
        if (!args || !read_line(args, marker, sizeof marker) ||
            !read_line(args, version, sizeof version) ||
            !read_line(args, lifetime_text, sizeof lifetime_text)) {
            fprintf(stderr, "could not read %s\n", path);
            return 2;
        }
        fclose(args);
    } else {
        fprintf(stderr, "usage: %s <marker> <version> <lifetime_seconds>\n",
                argv[0]);
        return 2;
    }

    FILE *f = fopen(marker, "w");
    if (f) {
        fputs(version, f);
        fclose(f);
    }

    int lifetime = atoi(lifetime_text);
    if (lifetime < 0) {
        lifetime = 0;
    }
    sleep((unsigned int)lifetime);
    return 0;
}
