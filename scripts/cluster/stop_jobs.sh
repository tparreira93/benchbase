#!/usr/bin/bash +x

ls OAR*stdout | sed 's/[^0-9]*//g' | while read i; do oardel $i; done