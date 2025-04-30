"""
Course    : CSE 351
Assignment: 02
Student   : Ash Jones (It might show as Kyle Jones in the system, but I go by Ash)

Instructions:
    - review instructions in the course
"""

# Don't import any other packages for this assignment
import os
import random
import threading
from money import *
from cse351 import Log

# ---------------------------------------------------------------------------
def main(): 

    print('\nATM Processing Program:')
    print('=======================\n')

    create_data_files_if_needed()

    # Load ATM data files
    data_files = get_filenames('data_files')
    # print(data_files)
    
    log = Log(show_terminal=True)
    log.start_timer()

    bank = Bank()

    # TODO - Add a ATM_Reader for each data file
     # Create a list of threads
    threads = []
    
    # Add an ATM_Reader for each data file, and start the thread
    for data_file in data_files:
        atm_reader = ATM_Reader(data_file, bank)
        threads.append(atm_reader)
        atm_reader.start()

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    test_balances(bank)

    log.stop_timer('Total time')


# ===========================================================================
class ATM_Reader(threading.Thread):
    def __init__(self, filename, bank):
        threading.Thread.__init__(self)
        self.filename = filename
        self.bank = bank

    def run(self):
        try:
            print(f"Thread starting: {self.filename}")
            with open(self.filename, 'r') as file:
                print(f"File opened: {self.filename}")
                for line in file:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue  # Skip empty lines and comments
                    try:
                        account_number, trans_type, amount = line.split(',')
                        account_number = int(account_number)
                        amount = float(amount)
                        if trans_type == 'd':
                            self.bank.deposit(account_number, amount)
                        elif trans_type == 'w':
                            self.bank.withdraw(account_number, amount)
                        else:
                            print(f"Unknown transaction type '{trans_type}' in file {self.filename}")
                    except ValueError as e:
                        print(f"Skipping bad line in {self.filename}: {line}")
            print(f"Thread finished: {self.filename}")
        except Exception as e:
            print(f"Error processing {self.filename}: {e}")


# ===========================================================================
class Account():
    # TODO - implement this class here
    def __init__(self, account_number):
        self.account_number = account_number
        self.lock = threading.Lock()
        self.balance = 0.0

    def deposit(self, amount):
        """Deposit amount into the account."""
        with self.lock:
            self.balance += amount

    def withdraw(self, amount):
        """Withdraw amount from the account, ensuring sufficient funds."""
        with self.lock:
            self.balance -= amount

    def get_balance(self):
        """Return the account's balance."""
        return self.balance


# ===========================================================================
class Bank():
    # TODO - implement this class here
    def __init__(self):
        self.accounts = {}
        self.lock = threading.RLock()  # Lock for thread-safe access to accounts

    def get_account(self, account_number):
        """Get an account by account number, creating it if necessary."""
        with self.lock:  # Ensure thread-safety when accessing accounts
            if account_number not in self.accounts:
                self.accounts[account_number] = Account(account_number)
            return self.accounts[account_number]

    def deposit(self, account_number, amount):
        """Deposit an amount into the given account."""
        with self.lock:  # Thread-safe deposit
            account = self.get_account(account_number)
            account.deposit(amount)

    def withdraw(self, account_number, amount):
        """Withdraw an amount from the given account."""
        with self.lock:  # Thread-safe withdrawal
            account = self.get_account(account_number)
            account.withdraw(amount)

    def get_balance(self, account_number):
        """Get the balance of a given account as a Money object."""
        with self.lock:
            account = self.get_account(account_number)
            cents = account.get_balance()  # returns int (e.g., 6155758)
            dollars = cents / 100      # convert to dollars (float)
            return Money(str(f"{dollars:.4f}"))     # Money expects a string input
        
    def get_balances(self):
        """Return a list of balances for all accounts, formatted as strings."""
        with self.lock:
            return [f"{account.get_balance():.2f}" for account in self.accounts]


# ---------------------------------------------------------------------------

def get_filenames(folder):
    """ Don't Change """
    filenames = []
    for filename in os.listdir(folder):
        if filename.endswith(".dat"):
            filenames.append(os.path.join(folder, filename))
    return filenames

# ---------------------------------------------------------------------------
def create_data_files_if_needed():
    """ Don't Change """
    ATMS = 10
    ACCOUNTS = 20
    TRANSACTIONS = 250000

    sub_dir = 'data_files'
    if os.path.exists(sub_dir):
        return

    print('Creating Data Files: (Only runs once)')
    os.makedirs(sub_dir)

    random.seed(102030)
    mean = 100.00
    std_dev = 50.00

    for atm in range(1, ATMS + 1):
        filename = f'{sub_dir}/atm-{atm:02d}.dat'
        print(f'- {filename}')
        with open(filename, 'w') as f:
            f.write(f'# Atm transactions from machine {atm:02d}\n')
            f.write('# format: account number, type, amount\n')

            # create random transactions
            for i in range(TRANSACTIONS):
                account = random.randint(1, ACCOUNTS)
                trans_type = 'd' if random.randint(0, 1) == 0 else 'w'
                amount = f'{(random.gauss(mean, std_dev)):0.2f}'
                f.write(f'{account},{trans_type},{amount}\n')

    print()

# ---------------------------------------------------------------------------
def test_balances(bank):
    """ Don't Change """

    # Verify balances for each account
    correct_results = (
        (1, '59362.93'),
        (2, '11988.60'),
        (3, '35982.34'),
        (4, '-22474.29'),
        (5, '11998.99'),
        (6, '-42110.72'),
        (7, '-3038.78'),
        (8, '18118.83'),
        (9, '35529.50'),
        (10, '2722.01'),
        (11, '11194.88'),
        (12, '-37512.97'),
        (13, '-21252.47'),
        (14, '41287.06'),
        (15, '7766.52'),
        (16, '-26820.11'),
        (17, '15792.78'),
        (18, '-12626.83'),
        (19, '-59303.54'),
        (20, '-47460.38'),
    )

    wrong = False
    for account_number, balance in correct_results:
        bal = bank.get_balance(account_number)
        print(f'{account_number:02d}: balance = {bal}')
        if Money(balance) != bal:
            wrong = True
            print(f'Wrong Balance: account = {account_number}, expected = {balance}, actual = {bal}')

    if not wrong:
        print('\nAll account balances are correct')



if __name__ == "__main__":
    main()

