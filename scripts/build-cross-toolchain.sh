#!/usr/bin/env bash
# ==============================================================================
# build-cross-toolchain.sh
# Cross-Compilation Toolchain Builder for SpacemiT X100 (RISC-V 64-bit + RVV 1.0)
# Designed for execution on x86_64 Debian GNU/Linux 13 (Trixie) - node 'rubicon'
# ==============================================================================

set -euo pipefail

PREFIX_BASE="${PREFIX_BASE:-/opt}"
BUILD_ROOT="${BUILD_ROOT:-/tmp/cross-toolchain-build}"
JOBS="${JOBS:-$(nproc)}"

GCC_INSTALL_DIR="${PREFIX_BASE}/cross-gcc-17"
LLVM_INSTALL_DIR="${PREFIX_BASE}/cross-llvm-24"

TARGET_TRIPLE="riscv64-unknown-linux-gnu"
TARGET_ARCH="rv64gcv"
TARGET_ABI="lp64d"

echo "======================================================================"
echo " SpacemiT X100 Cross-Toolchain Compilation (GCC 17 & LLVM 24)"
echo " Host:        $(uname -s) $(uname -m)"
echo " Target:      ${TARGET_TRIPLE} (${TARGET_ARCH}, ${TARGET_ABI})"
echo " CPU Flag:    -mcpu=spacemit-x100 (VLEN=256, RVV 1.0 +v)"
echo " Build Jobs:  ${JOBS}"
echo " Install Dir: ${PREFIX_BASE}"
echo "======================================================================"

# 1. Install Host Prerequisites
install_prereqs() {
    echo "==> [1/4] Checking host dependencies..."
    if command -v apt-get >/dev/null 2>&1; then
        sudo apt-get update -qq
        sudo apt-get install -y -qq \
            build-essential cmake ninja-build git python3 pkg-config \
            libgmp-dev libmpfr-dev libmpc-dev flex bison texinfo \
            libzstd-dev zlib1g-dev libncurses-dev \
            gcc-riscv64-linux-gnu g++-riscv64-linux-gnu \
            libc6-dev-riscv64-cross linux-libc-dev-riscv64-cross
    else
        echo "Note: Non-Debian host detected; ensure prerequisites are present."
    fi
}

# 2. Build GCC 17 Cross-Compiler from Git
build_gcc17() {
    echo "==> [2/4] Building GCC 17 from Git repository..."
    mkdir -p "${BUILD_ROOT}/gcc17"
    cd "${BUILD_ROOT}/gcc17"

    if [ ! -d "gcc" ]; then
        echo "Cloning GCC git repository (trunk/master)..."
        git clone --depth 1 https://github.com/gcc-mirror/gcc.git gcc
    else
        echo "Updating existing GCC git repository..."
        git -C gcc pull --ff-only || true
    fi

    mkdir -p build && cd build
    echo "Configuring GCC 17 for ${TARGET_TRIPLE}..."
    ../gcc/configure \
        --prefix="${GCC_INSTALL_DIR}" \
        --target="${TARGET_TRIPLE}" \
        --with-arch="${TARGET_ARCH}" \
        --with-abi="${TARGET_ABI}" \
        --with-tune=spacemit-x100 \
        --enable-languages=c,c++ \
        --disable-multilib \
        --disable-nls \
        --enable-shared \
        --enable-threads=posix \
        --enable-checking=release

    echo "Compiling GCC 17 (this may take 20-40 minutes)..."
    make -j"${JOBS}"
    sudo make install-strip
    echo "GCC 17 installed successfully into ${GCC_INSTALL_DIR}"
}

# 3. Build LLVM 24 Cross-Compiler from Git
build_llvm24() {
    echo "==> [3/4] Building LLVM 24 from Git repository..."
    mkdir -p "${BUILD_ROOT}/llvm24"
    cd "${BUILD_ROOT}/llvm24"

    if [ ! -d "llvm-project" ]; then
        echo "Cloning LLVM git repository (main branch)..."
        git clone --depth 1 https://github.com/llvm/llvm-project.git llvm-project
    else
        echo "Updating existing LLVM git repository..."
        git -C llvm-project pull --ff-only || true
    fi

    mkdir -p build && cd build
    echo "Configuring LLVM 24 via CMake Ninja..."
    cmake -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DCMAKE_INSTALL_PREFIX="${LLVM_INSTALL_DIR}" \
        -DLLVM_ENABLE_PROJECTS="clang;lld;compiler-rt" \
        -DLLVM_TARGETS_TO_BUILD="RISCV;X86" \
        -DLLVM_DEFAULT_TARGET_TRIPLE="${TARGET_TRIPLE}" \
        -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON \
        -DLLVM_OPTIMIZED_TABLEGEN=ON \
        -DLLVM_INCLUDE_BENCHMARKS=OFF \
        -DLLVM_INCLUDE_EXAMPLES=OFF \
        -DLLVM_INCLUDE_TESTS=OFF \
        ../llvm-project/llvm

    echo "Compiling LLVM 24 (this may take 30-50 minutes)..."
    ninja -j"${JOBS}"
    sudo ninja install
    echo "LLVM 24 installed successfully into ${LLVM_INSTALL_DIR}"
}

# 4. Generate SpacemiT X100 Toolchain Wrappers and Environment Profiles
generate_wrappers() {
    echo "==> [4/4] Generating SpacemiT X100 environment wrappers..."
    sudo mkdir -p "${PREFIX_BASE}/spacemit-x100-env"

    sudo tee "${PREFIX_BASE}/spacemit-x100-env/env.sh" > /dev/null << 'EOF'
# Source this file to activate SpacemiT X100 cross-compilation environment
export PATH="/opt/cross-llvm-24/bin:/opt/cross-gcc-17/bin:${PATH}"
export CC="clang"
export CXX="clang++"
export TARGET="riscv64gc-unknown-linux-gnu"
export CFLAGS="-target riscv64-unknown-linux-gnu -mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d"
export CXXFLAGS="-target riscv64-unknown-linux-gnu -mcpu=spacemit-x100 -march=rv64gcv -mabi=lp64d"
export RUSTFLAGS="-C target-feature=+v -C target-cpu=spacemit-x100"
export CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER="clang"
export CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_RUSTFLAGS="-C target-feature=+v -C target-cpu=spacemit-x100"
EOF

    echo "Environment activation profile written to ${PREFIX_BASE}/spacemit-x100-env/env.sh"
}

# Execution
case "${1:-all}" in
    prereqs)
        install_prereqs
        ;;
    gcc)
        install_prereqs
        build_gcc17
        ;;
    llvm)
        install_prereqs
        build_llvm24
        ;;
    wrappers)
        generate_wrappers
        ;;
    all)
        install_prereqs
        build_gcc17
        build_llvm24
        generate_wrappers
        echo "==> All toolchains built and installed successfully!"
        ;;
    *)
        echo "Usage: $0 [prereqs|gcc|llvm|wrappers|all]"
        exit 1
        ;;
esac
