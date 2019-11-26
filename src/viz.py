#!/usr/bin/env python3
"""visens"""
import visens

def main():
    """main"""
    path = "/data/kafka-to-nexus/"
    filename = path + "nicos_00000757.hdf"
    visens.tof(filename, save="/tmp/"+filename.replace(".hdf", ".png"))

if __name__ == "__main__":
    main()
