# hello_mpi.py
from mpi4py import MPI

comm = MPI.COMM_WORLD  # Get the global communicator
rank = comm.Get_rank()  # Get the rank of the current process
size = comm.Get_size()  # Get the total number of processes

print(f"Hello, World! I am process {rank} of {size}.")

