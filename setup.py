from setuptools import setup, Extension

native = Extension(
    name="systemd_dbus._native",
    sources=["src/systemd_dbus/_native/systemd_dbus.c"],
    libraries=["systemd"],
    extra_compile_args=["-fPIC"]
)

setup(ext_modules=[native])
