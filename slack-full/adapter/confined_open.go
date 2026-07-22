package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// openBeneath opens rel (a Clean, root-relative path containing no
// "..") beneath rootAbs via os.Root, the stdlib's openat(2)-backed
// traversal-confined API: every component resolves relative to the
// pinned root fd, so a directory swapped for a symlink mid-walk cannot
// redirect the open outside rootAbs — the portable equivalent of
// openat2(RESOLVE_BENEATH). The previous hand-rolled walk used raw
// syscall.Openat, which does not exist on darwin and made the adapter
// linux-only.
//
// os.Root alone is weaker than the old walk in two spots, and both
// gaps are closed here explicitly:
//
//   - The ROOT argument: os.OpenRoot follows a symlink at the root
//     path itself, so a root swapped for a symlink to /etc between the
//     caller's confinement check and this call would re-confine the
//     open beneath /etc. The root is therefore first opened with
//     plain open(2) + O_NOFOLLOW|O_DIRECTORY (portable — no openat
//     needed for the first component) and the os.Root handle must
//     match that fd's (dev, ino) identity.
//
//   - The LEAF: os.Root resolves in-root symlinks itself; passing
//     O_NOFOLLOW to Root.OpenFile does not reject them (verified
//     empirically on go1.26). realPath is EvalSymlinks-resolved before
//     it gets here, so ANY symlink at the leaf means the inode was
//     swapped in the race window. The leaf is Lstat'd first (symlink →
//     hard failure) and the opened file must match the Lstat'd
//     (dev, ino), so an open raced through a swapped-in link can never
//     return a different file than the one the caller validated —
//     including a different in-root file.
func openBeneath(rootAbs, rel string) (*os.File, error) {
	if rel == "" || rel == "." || filepath.IsAbs(rel) {
		return nil, fmt.Errorf("openBeneath: invalid relative path %q", rel)
	}
	comps := strings.Split(filepath.Clean(rel), string(filepath.Separator))
	for _, c := range comps {
		if c == "" || c == "." || c == ".." {
			return nil, fmt.Errorf("openBeneath: invalid path component %q in %q", c, rel)
		}
	}

	// Pin the verified root directory. O_NOFOLLOW applies to the final
	// component of rootAbs, so a root swapped for a symlink fails here
	// (ELOOP on linux, ENOTDIR via O_DIRECTORY on darwin).
	rootFD, err := syscall.Open(rootAbs, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: open root %q: %w", rootAbs, err)
	}
	defer syscall.Close(rootFD)
	var rootSt syscall.Stat_t
	if err := syscall.Fstat(rootFD, &rootSt); err != nil {
		return nil, fmt.Errorf("openBeneath: fstat root %q: %w", rootAbs, err)
	}

	root, err := os.OpenRoot(rootAbs)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: open root %q: %w", rootAbs, err)
	}
	defer root.Close()
	// os.OpenRoot re-resolved rootAbs independently of the pinned fd
	// above; require both opens to have landed on the same directory
	// inode so a swap between the two calls cannot substitute a
	// different root.
	rootFI, err := root.Stat(".")
	if err != nil {
		return nil, fmt.Errorf("openBeneath: stat root %q: %w", rootAbs, err)
	}
	if !sameInode(rootFI, &rootSt) {
		return nil, fmt.Errorf("openBeneath: root %q changed identity between opens", rootAbs)
	}

	// Leaf swap detection (see doc comment): a symlink at the leaf is
	// a hard failure, and the opened file must be the exact inode the
	// Lstat verified — covering both swap orders around the two calls.
	leafFI, err := root.Lstat(rel)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: lstat %q beneath %q: %w", rel, rootAbs, err)
	}
	if leafFI.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("openBeneath: %q beneath %q is a symlink", rel, rootAbs)
	}
	leafSt, ok := leafFI.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fmt.Errorf("openBeneath: lstat %q beneath %q: no unix stat", rel, rootAbs)
	}
	f, err := root.OpenFile(rel, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("openBeneath: open %q beneath %q: %w", rel, rootAbs, err)
	}
	openedFI, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("openBeneath: stat opened %q beneath %q: %w", rel, rootAbs, err)
	}
	if !sameInode(openedFI, leafSt) {
		_ = f.Close()
		return nil, fmt.Errorf("openBeneath: %q beneath %q changed identity during open", rel, rootAbs)
	}
	return f, nil
}

// sameInode reports whether fi refers to the same (dev, ino) identity
// as want. A FileInfo without a unix *syscall.Stat_t fails closed.
func sameInode(fi os.FileInfo, want *syscall.Stat_t) bool {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return st.Dev == want.Dev && st.Ino == want.Ino
}
