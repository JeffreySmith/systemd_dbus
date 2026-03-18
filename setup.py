from setuptools import setup, Extension, find_packages

native = Extension(
    name="systemd_dbus._sdbus",
    sources=["src/systemd_dbus/c/systemd_dbus.c", "src/systemd_dbus/c/dbus_api.c"],
    libraries=["systemd"],
    extra_compile_args=["-fPIC", "-Wall", "-Wextra", "-std=c99", "-Wundef"]
)


setup(
    name="systemd-dbus",
    version="0.2",
    description="Thin Python bindings for a minimal set of systemd service management commands, using sd-bus",
    author="Jeffrey Smith",
    author_email="jeffrey.smith@acceldata.io",
    license="Apache-2.0",
    python_requires=">=2.7",
    ext_modules=[native],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)
