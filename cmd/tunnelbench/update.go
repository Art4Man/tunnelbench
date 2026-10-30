package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const releases = "https://github.com/Art4Man/tunnelbench/releases"

var httpc = &http.Client{
	Timeout: 2 * time.Minute,
	// Keep the redirect of /releases/latest visible: its Location names the tag.
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if strings.HasSuffix(via[0].URL.Path, "/releases/latest") {
			return http.ErrUseLastResponse
		}
		return nil
	},
}

// cmdUpdate replaces this binary with the latest GitHub release, after checking
// it against the release's SHA256SUMS (the same steps as install.sh).
func cmdUpdate(args []string) error {
	fs := flag.NewFlagSet("update", flag.ExitOnError)
	force := fs.Bool("force", false, "replace this binary even if it is a development build or newer than the release")
	_ = fs.Parse(args)

	if runtime.GOOS != "linux" {
		return errors.New("releases are built for Linux only; on this machine use \"make build\"")
	}
	tag, err := latestTag()
	if err != nil {
		return unreachable(err)
	}
	replace, why := shouldReplace(version, tag, *force)
	if !replace {
		fmt.Println(why)
		return nil
	}

	archive := fmt.Sprintf("tunnelbench_linux_%s.tar.gz", runtime.GOARCH)
	fmt.Printf("Downloading %s %s\n", archive, tag)
	data, err := fetch(releases + "/download/" + tag + "/" + archive)
	if err != nil {
		return unreachable(err)
	}
	sums, err := fetch(releases + "/download/" + tag + "/SHA256SUMS")
	if err != nil {
		return unreachable(err)
	}
	if err := checkSum(data, sums, archive); err != nil {
		return err
	}
	bin, err := extract(data)
	if err != nil {
		return err
	}
	if err := replaceSelf(bin); err != nil {
		return err
	}
	fmt.Printf("Updated %s → %s. Run \"tunnelbench update\" on the other server too; both must run the same version.\n", version, tag)
	return nil
}

// shouldReplace decides whether to install release tag (e.g. "v0.1.1") over the
// running version. Release builds report "0.1.0" (GoReleaser drops the "v");
// builds from "make" report git describe output such as "v0.1.0-3-gaa2cdc9",
// "aa2cdc9-dirty" or "dev". It returns false and a message for the user when it
// keeps the current binary.
func shouldReplace(current, tag string, force bool) (bool, string) {
	latest := strings.TrimPrefix(tag, "v")
	if current == latest {
		return false, "Already up to date (" + current + ")."
	}
	if force {
		return true, ""
	}
	cur, ok := release(current)
	if !ok {
		return false, fmt.Sprintf("This is a development build (%s); keeping it. Run \"tunnelbench update -force\" to install release %s instead.", current, tag)
	}
	lat, ok := release(latest)
	if !ok {
		return false, fmt.Sprintf("Latest release tag %q is not a version; not updating.", tag)
	}
	for i := range cur {
		if cur[i] != lat[i] {
			if cur[i] > lat[i] {
				return false, fmt.Sprintf("This build (%s) is newer than release %s; keeping it. Use -force to install the release anyway.", current, tag)
			}
			return true, ""
		}
	}
	return false, "Already up to date (" + current + ")."
}

// release parses a plain "1.2.3" version; anything else (dev, git describe) is not a release.
func release(v string) ([3]int, bool) {
	var n [3]int
	_, err := fmt.Sscanf(v, "%d.%d.%d", &n[0], &n[1], &n[2])
	return n, err == nil && fmt.Sprintf("%d.%d.%d", n[0], n[1], n[2]) == v
}

func latestTag() (string, error) {
	resp, err := httpc.Get(releases + "/latest")
	if err != nil {
		return "", err
	}
	resp.Body.Close()
	loc := resp.Header.Get("Location")
	if !strings.Contains(loc, "/releases/tag/") {
		return "", fmt.Errorf("no release found (HTTP %d)", resp.StatusCode)
	}
	return path.Base(loc), nil
}

func fetch(url string) ([]byte, error) {
	resp, err := httpc.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: HTTP %d", url, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 200<<20))
}

func unreachable(err error) error {
	return fmt.Errorf("%w\nIf this server cannot reach GitHub (or its download host objects.githubusercontent.com), "+
		"copy tunnelbench_linux_<arch>.tar.gz, SHA256SUMS and install.sh from %s here with scp and run: sudo bash install.sh", err, releases)
}

func checkSum(data, sums []byte, name string) error {
	got := sha256.Sum256(data)
	sc := bufio.NewScanner(bytes.NewReader(sums))
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) == 2 && f[1] == name {
			if f[0] != hex.EncodeToString(got[:]) {
				return errors.New("checksum mismatch; not updating")
			}
			return nil
		}
	}
	return fmt.Errorf("%s is not listed in SHA256SUMS; not updating", name)
}

// extract returns the "tunnelbench" file from the release archive.
func extract(data []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err != nil {
			return nil, fmt.Errorf("tunnelbench binary not found in the archive: %w", err)
		}
		if h.Name == "tunnelbench" && h.Typeflag == tar.TypeReg {
			return io.ReadAll(io.LimitReader(tr, 200<<20))
		}
	}
}

// replaceSelf writes bin next to the running executable and renames it over,
// so a failed update never leaves a half-written binary.
func replaceSelf(bin []byte) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if exe, err = filepath.EvalSymlinks(exe); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(exe), ".tunnelbench-update-*")
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return fmt.Errorf("no permission to replace %s; run: sudo tunnelbench update", exe)
		}
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(bin); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmp.Name(), 0o755); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), exe)
}
