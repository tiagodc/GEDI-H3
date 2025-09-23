#!/usr/bin/env python3
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

# Import and run the module
if __name__ == '__main__':
    from gedih3 import gh3builder
    gh3builder._testit()