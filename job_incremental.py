import sys
import initiation
import incremental

def main():
    # Check command line arguments
    if len(sys.argv) != 2:
        print('Usage: python job_incremental.py <job_type>')
        return

    # Run the appropriate script
    job_type = sys.argv[1]
    if job_type == 'initialize':
        initiation.main()
    elif job_type == 'incremental':
        incremental.main()
    else:
        print(f'Unknown job type: {job_type}')

if __name__ == '__main__':
    main()