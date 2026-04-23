/*
 * Test stub for scripts/macos-bundle-swap-e2e.sh. Compiled at CI time
 * via clang and dropped into each test .app bundle as Contents/MacOS/
 * freenet-bin.
 *
 * Rationale: the updater script (scripts/macos-bundle-updater.sh) uses
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
 *
 * Writes <version> to <marker_file> on startup (so the test can assert
 * which bundle is actually running) then sleeps for <lifetime_seconds>
 * and exits.
 */

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

int main(int argc, char *argv[]) {
    if (argc != 4) {
        fprintf(stderr, "usage: %s <marker> <version> <lifetime_seconds>\n",
                argv[0]);
        return 2;
    }

    FILE *f = fopen(argv[1], "w");
    if (f) {
        fputs(argv[2], f);
        fclose(f);
    }

    int lifetime = atoi(argv[3]);
    if (lifetime < 0) {
        lifetime = 0;
    }
    sleep((unsigned int)lifetime);
    return 0;
}
