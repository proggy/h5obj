# To Do

- make get-method decode (so far it just calls the original get-method)


## Submodule tools

- add --parents option to h5mkgrp and h5rmgrp
- with h5ls, don't list groups; do not list content of groups like "find", but more like "ls" (without -d option)
- implement long option for h5ls, the default being False (if True, list objtype, datatype and maybe the data itself)
- implement h5ll (ls but in long listing format)
- do not have to use "," as a separator in "h5cp", improve frog
