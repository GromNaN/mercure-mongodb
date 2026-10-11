#!/bin/sh
# Collect the license and notice files that a distribution of the hub binary
# must carry, and write them under the given directory. The release archives
# and the container image copy that directory in.
#
# Nothing here is committed: the tree is rebuilt from the Go module cache on
# each release. The Go toolchain is the only requirement.
#
# The set is the full dependency graph of the hub entry point. It is a superset
# of the packages that the build tags actually link, and being over-inclusive
# is the safe direction for a notice file.
#
# Usage: collect-licenses.sh [output directory]   (default: licenses)
set -eu

out=${1:-licenses}
main=$(go list -m)

rm -rf "$out"
mkdir -p "$out"

# One directory per module, named after the module path. The file names differ
# from one project to the next, so match the usual set.
go list -deps -f '{{with .Module}}{{.Path}} {{.Dir}}{{end}}' ./caddy/mercure |
	sort -u |
	while read -r path dir; do
		[ -n "$path" ] || continue
		[ "$path" != "$main" ] || continue
		[ -n "$dir" ] && [ -d "$dir" ] || continue

		for file in "$dir"/LICENSE* "$dir"/LICENCE* "$dir"/COPYING* "$dir"/COPYRIGHT* \
			"$dir"/NOTICE* "$dir"/PATENTS* "$dir"/THIRD-PARTY*; do
			[ -f "$file" ] || continue
			mkdir -p "$out/$path"
			cp "$file" "$out/$path/"
		done
	done

files=$(find "$out" -type f | wc -l | tr -d ' ')
modules=$(find "$out" -type f -exec dirname {} \; | sort -u | wc -l | tr -d ' ')
echo "collected $files license files from $modules modules into $out"
