from setuptools import setup, Extension, find_packages

native = Extension(
    name="systemd_dbus._sdbus",
    sources=["src/systemd_dbus/_native/systemd_dbus.c"],
    libraries=["systemd"],
    extra_compile_args=["-fPIC", "-Wall", "-Wextra"]
)

setup(
    ext_modules=[native],
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)

