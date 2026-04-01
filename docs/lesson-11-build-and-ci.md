# Lesson 11: Build System & CI Workflows

## Learning Goals

After this lesson you should be able to:

- Understand the full build pipeline from source code to a distributable JAR
- Trace how Velox C++ source is fetched, compiled, and bundled into the Java artifact
- Explain each CI workflow and what it validates
- Know how the Velox version is pinned and automatically bumped
- Set up a local development environment for building Velox4J

---

## 11.1 Build Pipeline Overview

Building Velox4J involves two languages and two build systems working together:

```
                    mvn clean install
                          │
            ┌─────────────┴─────────────┐
            │                           │
    generate-resources phase        compile + test phase
            │                           │
    exec-maven-plugin runs          javac compiles Java
    build.sh                        surefire runs tests
            │
            ▼
    CMake builds C++
    (fetches Velox from GitHub,
     compiles libvelox4j.so,
     installs with all deps)
            │
            ▼
    Native libs land in
    src/main/cpp/build/dist/lib/
            │
            ▼
    Maven bundles them into JAR at
    velox4j-lib/{os.name}/{os.arch}/
```

The key insight: **Maven drives the C++ build**. When you run `mvn clean install`, Maven's `exec-maven-plugin` calls `build.sh` during the `generate-resources` phase, which triggers the entire CMake build. The resulting `.so` files are then packaged into the JAR as resources.

---

## 11.2 Step 1: Velox Source Fetching

Velox4J doesn't include Velox source code in its repository. Instead, it pins a specific Velox commit and downloads it at build time.

### The pin files

```
src/main/cpp/
├── velox-ref.txt        # Git commit hash (e.g., "1e19ac67f469b5c3ca2a3d2f0a27a33039e311ab")
├── velox-ref-md5.txt    # MD5 checksum of the source zip
```

### How CMake fetches Velox

```cmake
# CMakeLists.txt
file(STRINGS ${VELOX_REF_FILE} VELOX_REF)                    # read commit hash
file(STRINGS ${VELOX_REF_MD5_FILE} VELOX_SOURCE_URL_MD5)      # read MD5
set(VELOX_SOURCE_URL
    "https://github.com/facebookincubator/velox/archive/${VELOX_REF}.zip")

FetchContent_Declare(velox
    URL ${VELOX_SOURCE_URL}
    URL_HASH MD5=${VELOX_SOURCE_URL_MD5})
FetchContent_MakeAvailable(velox)
```

CMake's `FetchContent` downloads the Velox source zip, verifies the MD5, and unpacks it into `src/main/cpp/build/_deps/velox-src/`. This directory contains the full Velox source code and build system.

### Why pin a specific commit?

- **Reproducibility**: Every build uses the exact same Velox version
- **Stability**: Velox's `main` branch changes daily; pinning avoids surprise breakage
- **MD5 verification**: Ensures the downloaded archive is intact

---

## 11.3 Step 2: C++ Compilation (build.sh)

The `src/main/cpp/build.sh` script orchestrates the C++ build:

```bash
# 1. Configure: CMake generates build files
cmake -DCMAKE_BUILD_TYPE=Release \
      -DVELOX4J_ENABLE_CCACHE=ON \
      -DVELOX4J_BUILD_TESTING=OFF \
      -DVELOX4J_INSTALL_DESTINATION="$INSTALL_DESTINATION" \
      -S "$SOURCE_DIR" -B "$BUILD_DIR"

# 2. Build: compile libvelox4j.so (links Velox + JniHelpers)
cmake --build "$BUILD_DIR" --target velox4j_shared -j "$NUM_THREADS"

# 3. Install: copy libvelox4j.so + all shared library dependencies
cmake --install "$BUILD_DIR" --component velox4j
```

### What gets compiled

The CMake build compiles three things:

1. **Velox** itself — the full execution engine (as a mono shared library `libvelox.so`)
2. **JniHelpers** — Spotify's JNI helper library (from GitHub, also fetched via `FetchContent`)
3. **Velox4J C++** — the ~25 source files in `src/main/cpp/main/` that form the JNI bridge

These are linked into a single shared library: **`libvelox4j.so`**.

### The install step

The CMake install step is clever — it uses `file(GET_RUNTIME_DEPENDENCIES)` to find **all** shared libraries that `libvelox4j.so` depends on (transitive dependencies), then copies them to the install directory:

```cmake
# main/CMakeLists.txt — install step
file(GET_RUNTIME_DEPENDENCIES
    RESOLVED_DEPENDENCIES_VAR 3rd_deps
    LIBRARIES $<TARGET_FILE:velox4j_shared>
    PRE_EXCLUDE_REGEXES ${VELOX4J_3RD_EXCLUSIONS}    # exclude system libs
)
# Copy each dependency to the install directory
foreach(dep IN LISTS 3rd_deps)
    file(INSTALL DESTINATION ${VELOX4J_INSTALL_DESTINATION} ...)
endforeach()
```

System libraries (glibc, libstdc++, libpthread, etc.) are excluded — they're expected to exist on the target system.

### Making it portable with patchelf

After installing, `build.sh` runs `patchelf` to set the RPATH of every `.so` file to `$ORIGIN`:

```bash
# Remove existing RPATH
patchelf --remove-rpath "$file"

# Set RPATH to $ORIGIN (find libraries in the same directory)
patchelf --set-rpath '$ORIGIN' "$file"
```

`$ORIGIN` means "the directory containing this `.so` file". This way, when Java extracts the `.so` files to a temp directory at runtime, `libvelox4j.so` can find all its dependencies (like `libvelox.so`) in the same directory — no system-wide installation needed.

**Source:** `src/main/cpp/build.sh`

---

## 11.4 Step 3: Bundling into the JAR

Maven bundles the compiled native libraries into the JAR as resources:

```xml
<!-- pom.xml -->
<resource>
    <targetPath>velox4j-lib/${os.name}/${os.arch}</targetPath>
    <directory>${project.basedir}/src/main/cpp/build/dist/lib</directory>
</resource>
```

For a Linux x86-64 build, the native libs end up at `velox4j-lib/Linux/amd64/` inside the JAR. At runtime, `JniLibLoader` extracts them to a temp directory and calls `System.load()` (see Lesson 6).

---

## 11.5 Step 4: Skipping the C++ Build

If you're only working on Java code and have already built the C++ libraries, you can skip the C++ build:

```bash
mvn clean test -Dskip.cpp.build=true
```

The `skip.cpp.build` property (default: `false`) controls the `exec-maven-plugin` that runs `build.sh`. Setting it to `true` skips the entire CMake build and uses whatever native libraries were previously compiled.

The CI workflows use this pattern: build C++ once in a setup step, then run Java tests with `-Dskip.cpp.build`:

```yaml
# ut-java.yml
- name: Build C++ libraries
  run: mvn clean generate-resources          # builds C++ only
- name: Run UTs
  run: mvn clean test -Dskip.cpp.build       # Java tests, skip C++ rebuild
```

---

## 11.6 CI Workflows

Velox4J has six GitHub Actions workflows:

### 1. Java UT (`ut-java.yml`)

**Triggers:** Every push to `main` and every PR.

**Jobs:**

| Job | Container | What it does |
|-----|-----------|-------------|
| `ut-ubuntu24` | Ubuntu 24.04 (Docker) | Setup → build C++ → run Java tests → run Java tests with Arrow 7.0.0 |
| `ut-centos7` | CentOS 7 (Docker) | Setup → build C++ → run Java tests |

The Ubuntu job tests compatibility with the latest OS. The CentOS 7 job tests compatibility with the oldest supported glibc (2.17) — this is the environment used for release JARs.

Testing with Arrow 7.0.0 alongside the default Arrow 17.0.0 verifies backward compatibility with older Arrow versions.

**Ccache:** Both jobs use `ccache` to cache C++ compilation results between runs. The cache key includes the git SHA, so incremental builds are fast.

### 2. Java UT — Amazon Linux 2023 (`ut-java-al2023.yml`)

**Triggers:** Every push to `main` and every PR.

Runs on the `amazonlinux:2023` Docker image. Uses `setup-al2023.sh` for environment setup, then builds C++ and runs Java tests. This validates compatibility with Amazon Linux — important for AWS deployment scenarios.

### 3. C++ UT (`ut-cpp.yml`)

**Triggers:** Every push to `main` and every PR.

Runs the C++ unit tests (GTest) using `test.sh`:

```bash
cmake -DVELOX4J_BUILD_TESTING=ON ...    # enable test targets
cmake --build "$BUILD_DIR" -j ...        # build everything including tests
cd build/test && ctest -V                # run tests
```

The C++ tests cover:
- Query serde round-trip (`QuerySerdeTest.cc`)
- Query execution (`QueryTest.cc`)
- Blocking queue (`BlockingQueueTest.cc`)

### 4. Code Format (`format.yml`)

**Triggers:** Every push to `main` and every PR.

Checks both C++ and Java code formatting:

| Check | Tool | Target |
|-------|------|--------|
| C++ code style | `clang-format-18` | `*.h`, `*.cc`, `*.cpp` |
| CMake style | `cmake-format` | `CMakeLists.txt`, `*.cmake` |
| Java code style | Maven Spotless (Google Java Format) | `*.java` |

To fix formatting locally:
```bash
bash .github/workflows/scripts/format/format.sh -fix    # requires Docker
```

### 5. Publish Snapshot (`pub-snapshot.yml`)

**Triggers:** Manual (`workflow_dispatch`) only.

Builds the native library on CentOS 7 (for maximum glibc compatibility), then publishes the JAR to Maven Central's snapshot repository with GPG signing.

```yaml
- name: Build C++ (CentOS 7)
  run: mvn generate-resources        # builds libvelox4j.so

- name: Publish
  run: mvn -P release deploy -Dskip.cpp.build=true   # Java-only, with GPG signing
```

### 6. Dependency Bot (`bot-dep.yml`)

**Triggers:** Daily cron at 04:00 UTC, or manual.

Automatically bumps the pinned Velox version:

```bash
# bump-velox.sh
LATEST_COMMIT_HASH="$(git ls-remote https://github.com/facebookincubator/velox.git refs/heads/main | awk '{print $1}')"
echo "$LATEST_COMMIT_HASH" > velox-ref.txt
# ... compute and write MD5 ...
```

Then creates a PR with the updated `velox-ref.txt` and `velox-ref-md5.txt`, and auto-merges it if CI passes. This keeps Velox4J tracking Velox's latest `main` branch.

---

## 11.7 Environment Setup Scripts

The CI uses Docker containers with setup scripts that install all build dependencies.

### CentOS 7 (`scripts/common/setup-centos7.sh`)

This is the **release build environment**. CentOS 7 has glibc 2.17 — the oldest glibc that Velox4J targets. Libraries built here work on almost any modern Linux distribution.

What it installs:
- **GCC 11** via devtoolset-11 (CentOS 7's default GCC is too old)
- **CMake 3.28** via pip (CentOS 7's default is too old)
- **OpenSSL 1.1.1** from source (CentOS 7's default is too old)
- **Flex 2.6**, **ICU 72.1** from source
- **Java 11**, **Maven 3.9**
- **patchelf** for RPATH patching
- **ccache** for build caching
- Various compression libraries: lz4, lzo, zstd, snappy

### Ubuntu 24.04 (`scripts/common/setup-ubuntu24.sh`)

A simpler setup — most dependencies are available from apt:

```bash
apt-get install -y gcc-11 g++-11 cmake maven openjdk-11-jdk patchelf ccache ...
```

### Amazon Linux 2023 (`scripts/common/setup-al2023.sh`)

Amazon Linux 2023 is Fedora-based (uses `dnf`), with glibc 2.34 and GCC 11 out of the box. It's a middle ground between CentOS 7 (everything is old) and Ubuntu 24 (everything is from apt):

- **GCC 11**: ships by default — no devtoolset needed
- **OpenSSL 3.0, Flex 2.6, ICU 67+, Git 2.40+**: all available from dnf — no from-source builds
- **CMake 3.28**: must install from pip (AL2023 ships 3.22, too old)
- **Maven**: must install manually (not in AL2023 repos)
- **Java 11**: via Amazon Corretto (`java-11-amazon-corretto-devel`)
- **patchelf, ccache**: must install from GitHub releases / fallback

This setup is significantly simpler than CentOS 7 because AL2023 has modern system libraries. The corresponding workflow (`ut-java-al2023.yml`) uses the `amazonlinux:2023` Docker image.

---

## 11.8 The CMake Configuration in Detail

The root `CMakeLists.txt` sets up the build:

```cmake
# C++20 required (Velox uses it)
set(CMAKE_CXX_STANDARD 20)

# Velox build options
set(VELOX_MONO_LIBRARY ON)    # build Velox as a single shared library
set(VELOX_BUILD_SHARED ON)    # shared, not static
set(VELOX_ENABLE_ARROW ON)    # enable Arrow integration

# Fetch Velox from GitHub
FetchContent_Declare(velox URL ${VELOX_SOURCE_URL} URL_HASH MD5=${MD5})
FetchContent_MakeAvailable(velox)

# Fetch JniHelpers from GitHub
FetchContent_Declare(JniHelpers GIT_REPOSITORY ... GIT_TAG ...)
FetchContent_MakeAvailable(JniHelpers)

# Build libvelox4j.so
add_library(velox4j_shared SHARED ${VELOX4J_SOURCES})
target_link_libraries(velox4j_shared PUBLIC velox JniHelpers JNI::JNI)
```

Key options:
- **`VELOX_MONO_LIBRARY`**: Builds all of Velox as a single `libvelox.so` instead of many small libraries. Simplifies dependency management.
- **`VELOX4J_BUILD_TESTING`**: Set to `ON` for C++ tests, `OFF` for production builds.
- **`VELOX4J_ENABLE_CCACHE`**: Uses ccache to speed up incremental builds.

---

## 11.9 Local Development Workflow

### First-time full build

```bash
# Requires: GCC 11+, CMake 3.28+, JDK 8+, Maven, patchelf
mvn clean install
```

This takes a long time (30+ minutes) because it downloads and compiles Velox from source.

### Subsequent Java-only development

```bash
# Skip C++ build (uses previously compiled native libs)
mvn clean test -Dskip.cpp.build=true
```

### C++ development

```bash
# Build and test C++ only
cd src/main/cpp
bash test.sh    # builds with testing enabled, runs ctest
```

### Fix code formatting

```bash
# Requires Docker
bash .github/workflows/scripts/format/format.sh -fix
```

### Check formatting without fixing

```bash
bash .github/workflows/scripts/format/format.sh -check
```

---

## 11.10 Key Takeaways

1. **Maven drives the entire build**. `mvn clean install` triggers `build.sh` → CMake → compile Velox + Velox4J C++ → install `.so` files → bundle into JAR.

2. **Velox is fetched at build time** from GitHub via CMake `FetchContent`, pinned to a specific commit in `velox-ref.txt`.

3. **patchelf makes the build portable**. Setting `$ORIGIN` RPATH means the native libraries find each other relative to their own location, with no system-wide installation.

4. **CentOS 7 is the release platform**. Building on glibc 2.17 ensures compatibility with nearly all Linux distributions.

5. **Six CI workflows**: Java UT (Ubuntu + CentOS), Java UT (Amazon Linux 2023), C++ UT (CentOS), Code Format, Snapshot Publish, and Velox Version Bot.

6. **Daily Velox bumps** keep the project tracking Velox's `main` branch. The bot creates auto-merge PRs.

7. **`-Dskip.cpp.build=true`** is your friend for Java-only development — skips the expensive C++ compilation.

---

## 11.11 Exercises

1. **Trace the build**: Starting from `mvn clean install`, list every major step that happens. Which phase triggers the C++ build? Where do the `.so` files end up in the JAR?

2. **Read the pin files**: Open `velox-ref.txt`. What Velox commit is currently pinned? How would you find the corresponding Velox source on GitHub?

3. **Why CentOS 7?**: The release JAR is built on CentOS 7. Why not Ubuntu 24.04? What would happen if you built on Ubuntu 24.04 and ran on CentOS 7?

4. **RPATH experiment**: If `patchelf` didn't set `$ORIGIN` RPATH, what would happen when Java calls `System.load("libvelox4j.so")` from a temp directory?

5. **Bump Velox manually**: Read `bump-velox.sh`. If you wanted to pin Velox to a specific commit (instead of latest main), what two files would you edit?
