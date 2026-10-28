load("@prelude//:prelude.bzl", "native")
load("@prelude//rust:cargo_package.bzl", "apply_platform_attrs", "apply_platform_attrs_for_buildscript_build", "get_reindeer_platform_names")

def _probe_impl(ctx):
    return [DefaultInfo()]

_probe = rule(
    impl = _probe_impl,
    attrs = {"values": attrs.list(attrs.string(), default = [])},
)

def platform_probe(name, platform = {}, buildscript = False, **kwargs):
    apply = apply_platform_attrs_for_buildscript_build if buildscript else apply_platform_attrs
    _probe(name = name, **apply(platform, kwargs))

def buildscript_platform(name, target):
    platform_index = list(get_reindeer_platform_names()).index(target)
    native.platform(
        name = name,
        constraint_values = [
            "prelude//os/constraints:os[macos]",
            "prelude//cpu/constraints:cpu[arm64]",
            "prelude//rust/buildscript:buildscript_for_platform[{}]".format(platform_index),
        ],
    )
