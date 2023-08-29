set(src
  in_tenzir.c
  )

FLB_PLUGIN(in_tenzir "${src}" "")
# TODO: create a minimal libtenzir with C ABI
#target_link_libraries(flb-plugin-in_tenzir -ltenzir)
