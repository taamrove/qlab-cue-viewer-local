# Bundle the SwiftPM executable as a real macOS .app and produce a zip you
# can hand to someone else. No Xcode project file involved.

APP_NAME  = QlabCueViewer
APP       = $(APP_NAME).app
DIST      = dist
# Pulled from Info.plist so a single edit there bumps both the bundle and
# the zip filename.
VERSION   = $(shell /usr/libexec/PlistBuddy -c "Print CFBundleShortVersionString" Info.plist)

.PHONY: help app universal run zip clean

help:
	@echo "Targets:"
	@echo "  make app         build .app for THIS Mac's architecture (fast iteration)"
	@echo "  make universal   build universal (arm64 + x86_64) .app — runs on any Mac"
	@echo "  make run         build & launch — opens the menu bar app"
	@echo "  make zip         produce dist/$(APP_NAME)-<version>.zip to share"
	@echo "  make clean       remove build artifacts"

# Single-arch build — fast, for development on the build machine.
app:
	@swift build -c release 2>&1 | tail -3
	@$(MAKE) -s _bundle BINARY=.build/release/$(APP_NAME)

# Universal binary — slower but the resulting .app runs on both Apple Silicon
# and Intel Macs. Use this for the build you hand to someone else.
universal:
	@echo "Building universal binary (arm64 + x86_64)…"
	@swift build -c release --arch arm64 --arch x86_64 2>&1 | tail -3
	@$(MAKE) -s _bundle BINARY=.build/apple/Products/Release/$(APP_NAME)

# Internal: wrap a binary as a .app bundle. Ad-hoc codesigned so macOS will
# launch it; the recipient still has to right-click → Open the first time
# because it isn't notarized with a Developer ID.
_bundle:
	@rm -rf $(APP)
	@mkdir -p $(APP)/Contents/MacOS
	@cp $(BINARY) $(APP)/Contents/MacOS/$(APP_NAME)
	@cp Info.plist $(APP)/Contents/Info.plist
	@codesign --force --deep --sign - $(APP) >/dev/null 2>&1
	@file $(APP)/Contents/MacOS/$(APP_NAME) | sed 's|.*: |  arch:    |'
	@du -h $(APP)/Contents/MacOS/$(APP_NAME) | awk '{print "  binary:  "$$1}'
	@echo "✔ $(APP)"

run: app
	@pkill -f $(APP_NAME) 2>/dev/null || true
	@sleep 0.5
	@open ./$(APP)
	@echo "✔ launched ($(APP_NAME) in menu bar)"

zip: universal
	@mkdir -p $(DIST)
	@rm -f $(DIST)/$(APP_NAME)-$(VERSION).zip
	@/usr/bin/ditto -c -k --keepParent $(APP) $(DIST)/$(APP_NAME)-$(VERSION).zip
	@echo ""
	@ls -lh $(DIST)/$(APP_NAME)-$(VERSION).zip | awk '{print "✔ "$$NF"  ("$$5")"}'

clean:
	@rm -rf .build $(APP) $(DIST)
	@echo "✔ cleaned"
