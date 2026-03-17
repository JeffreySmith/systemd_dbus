from setuptools import setup, Extension, find_packages

native = Extension(
    name="systemd_dbus._sdbus",
    sources=["src/systemd_dbus/c/systemd_dbus.c", "src/systemd_dbus/c/dbus_api.c"],
    libraries=["systemd"],
    extra_compile_args=["-fPIC", "-Wall", "-Wextra", "-std=c99", "-Wundef"]
)

setup(
    ext_modules=[native],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)

