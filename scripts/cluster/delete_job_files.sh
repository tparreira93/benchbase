#!/usr/bin/bash +x

ls OAR*std* | while read i; do rm $i; done