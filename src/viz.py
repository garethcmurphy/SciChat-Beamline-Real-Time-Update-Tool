#!/usr/bin/env python3
# """visens"""
import visens

filename = "nicos_00000757.hdf"
visens.tof(filename, save="/tmp/"+filename.replace(".hdf", ".png"))
