# Why use SQLite?
We need to store all the "open" transactions (more on that later) and all the
account balances while we are processing the file. The amount of transactions
is 2\*\*32 (unique u32 ids) and the minimum number of bytes to store is 4 bytes
(for the id) meaning the memory footprint would be on the order of gigabytes.
Because we do not have requirements on the machine, we assume commodity
hardware, so we need to offload the data to disk. SQLite will do it
automatically and handle caching for us among many other benefits.

In the future, when we need to handle concurrent streams we need transactional
semantics to check balances and perform the appropriate operations atomically.
It is far more reliable to use an established library than to create our own
locking and recovery protocols. SQLite is not ideal for very high performance
because it is single writer but without knowing more about requirements, it is
more than enough.

Next, SQLite being embedded has no problem dealing with doing a lot of
different queries instead of long complex ones. This allows us to write most of
the business logic in Rust without performance penalty.

Lastly, SQLite is extremely robust and has been tested in a vast amount of
environments to be proven correct. Our custom implementation would need
extensive testing across many platforms.

# Error handling and panics
Because this tool is used for analysis and correctness is what is most
important, unexpected errors are considered fatal and asserts are used to catch
run-time bugs. The philosophy is to fail early when encountering a broken
assumption instead of producing a wrong result.

Error reporting can be improved but more details about the use-case would be
necessary in order to know who the final user is.

# Negative balances
If a transactions is disputed and the user has already spent the funds, the
account balance is allowed to go negative and the account is overdrawn.

# Transaction states
A transaction can be in three states:
    Open -> Pending -> Closed
where Open is the initial state. Once a Dispute is started a transaction goes
to Pending and once it is settled (Chargeback or Resolve) it goes to Closed. No
dispute can be opened again on the same transaction, this is similar to what
some payment processors do.

# On disputes
With the wording on the requirements and the fact that we are using
single-entry accounting, it is hard for me to know if disputes to both deposits
and withdrawals make sense. Based on the wording under chargeback and the use
of disputes in other payment platforms, only disputes for deposits are going to
be allowed. The way I understand it, disputing a withdrawal makes sense only at
the other end where the funds were deposited and can be held.

# AI
I have only used AI to generate some test files CSVs and to ask it around
certain operations in serde/csv like one would use Google. Lastly, I have asked
the AI to review the patch to see grammatical, logic, mistakes; but all fixes
were written by me.

# Decimal precision
To make the code shorter and not introduce another dependency, I have chosen to
represent decimals as i64 numbers where the last 4 digits in base 10 represent
the decimal part. This makes the arithmetic very easy and storage and retrieval
in the database.

If this was a real implementation, it would be better to use a tried and tested
existing crate rather than implementing our own (in the majority of cases).

# Tests
The worst part of the code is probably the tests. In a proper application all
the business logic should be tested, as well as all business logic errors. Some
parts of the code like the number conversion to decimal with 4 digit precision
could be fuzzed instead of tested with a handful of inputs.

With that said, I do like the test setup that matches Go's philosophy where
most tests use a table, in this case files representing input and output, so
new scenarios can be added easily and setup/teardown are not duplicated.
