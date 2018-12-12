import unittest
from integrator import *


class TestTransactions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        df_transactions = get_transactions_df()
        df_customers = get_customers_df()

        print(df_transactions.head())
        print(df_customers.head())

        merged = get_merged_dataframe(
            df_transactions,
            df_customers
        )
        cls.transactions = get_cleaned_transactions(merged)
        cls.customers = get_cleaned_customers(merged)

    def test_validate_customer_transaction_totals(self):
        '''
        count whether total number of transactions is valid
        '''
        self.assertTrue(len(TestTransactions.transactions), 14)


if __name__ == "__main__":
    unittest.main()
