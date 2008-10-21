#!/usr/bin/env python

from string import Template

def fill_in_template(templ_file, config_dict):
    return Template(open(templ_file).read()).substitute(config_dict)

def write_final_config(config, final_path):
    f = open(final_path, 'w')
    f.write(config)
    f.close()


if __name__ == "__main__":
    import sys
    templ_file = sys.argv[1]
    config_dict = eval(sys.argv[2])
    final_path = sys.argv[3]
    config = fill_in_template(templ_file, config_dict)
    write_final_config(config, final_path)
