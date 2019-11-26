#!/usr/bin/env python3
"""visens"""
import visens

def main():
    """main"""
    path = "/data/kafka-to-nexus/"
    filename = "nicos_00001500.hdf"
    visens.preview(path+filename, log=True, save=filename.replace(".hdf", ".png"))


if __name__ == "__main__":
    main()
