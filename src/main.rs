use std::env;
use std::fs::{self, File};
use std::io::{self, ErrorKind};
use std::path::Path;
use std::process;

use anyhow::{Context, Result, anyhow};
use rusqlite::{Connection, Transaction, named_params, params};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "lowercase")]
enum OperationType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

// Note: If I had more time I would implement Add,Sub,Ord,etc. instead of getting the inner
// value. This is more robust and uses the type system to prevent mistakes.
#[derive(Clone, Copy, Debug)]
struct PreciseAmount(i64);

impl PreciseAmount {
    fn from_str(s: &str) -> Result<PreciseAmount> {
        let parts: Vec<_> = s.split(".").collect();
        if parts.is_empty() || parts.len() > 2 {
            return Err(anyhow!("invalid number"));
        }
        let whole: i64 = parts[0].parse()?;
        let decimal: i64 = if parts.len() == 2 {
            if parts[1].len() > 4 {
                return Err(anyhow!("invalid number: too many decimal digits"));
            }
            // We need to add zeros to the right of the number for it to make sense:
            // * 0.1 -> "1" -> 1000
            // * 0.0001 -> "0001" -> 1
            let padded = format!("{:0<4}", parts[1]);
            padded.parse()?
        } else {
            0
        };
        let is_negative = parts[0].starts_with("-");
        let total_unsigned = whole.abs() * 10_i64.pow(4) + decimal;
        let total = if is_negative {
            -1 * total_unsigned
        } else {
            total_unsigned
        };
        Ok(PreciseAmount(total))
    }

    fn to_string(&self) -> String {
        let decimal = self.0.abs() % 10_i64.pow(4);
        let whole = (self.0.abs() - decimal) / 10_i64.pow(4);
        format!(
            "{}{}.{:04}",
            if self.0.is_negative() { "-" } else { "" },
            whole,
            decimal
        )
    }
}

impl<'de> Deserialize<'de> for PreciseAmount {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        PreciseAmount::from_str(&s).map_err(de::Error::custom)
    }
}

impl Serialize for PreciseAmount {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Debug)]
struct ClientId(u16);

#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
struct TxId(u32);

#[derive(Serialize, Deserialize, Debug)]
struct Operation {
    #[serde(rename = "type")]
    typ: OperationType,
    amount: Option<PreciseAmount>,
    // Per requirements:
    client: ClientId,
    tx: TxId,
}

#[derive(Serialize, Deserialize, Debug)]
struct Balance {
    client: ClientId,
    available: PreciseAmount,
    held: PreciseAmount,
    total: PreciseAmount,
    locked: bool,
}

enum TxState {
    Open,
    Pending,
    Closed,
}

impl TxState {
    fn to_db(&self) -> &str {
        match self {
            Self::Open => "open",
            Self::Pending => "pending",
            Self::Closed => "closed",
        }
    }

    fn from_db(s: &str) -> Self {
        match s {
            "open" => Self::Open,
            "pending" => Self::Pending,
            "closed" => Self::Closed,
            // This is a valid panic because the db guarantees the values in the column belong to
            // the set.
            _ => panic!("internal error: invalid state value from db"),
        }
    }
}

fn exec<R, W, P>(database_path: P, input: R, output: W) -> Result<()>
where
    R: io::Read,
    W: io::Write,
    P: AsRef<Path>,
{
    // Create and set up database.
    if let Err(err) = fs::remove_file(&database_path)
        && err.kind() != ErrorKind::NotFound
    {
        return Err(err.into());
    }
    let mut conn = Connection::open(database_path).context("cannot open database connection")?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .context("internal error: cannot set foreign_keys")?;
    conn.execute_batch(SCHEMA)
        .context("internal error: cannot set database schema")?;

    // Read csv and execute each line.
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(input);
    for line in reader.deserialize() {
        let op: Operation = line?;
        match op {
            Operation {
                typ: OperationType::Withdrawal,
                amount: Some(amount),
                client,
                tx,
            } => handle_withdrawal(&mut conn, amount, client, tx)
                .context("internal error: cannot perform withdrawal")?,
            Operation {
                typ: OperationType::Deposit,
                amount: Some(amount),
                client,
                tx,
            } => handle_deposit(&mut conn, client, amount, tx)
                .context("internal error: cannot perform deposit")?,
            Operation {
                typ: OperationType::Dispute,
                amount: None,
                client,
                tx,
            } => handle_dispute(&mut conn, client, tx)
                .context("internal error: cannot perform dispute")?,
            Operation {
                typ: OperationType::Resolve,
                amount: None,
                client,
                tx,
            } => handle_resolve(&mut conn, client, tx)
                .context("internal error: cannot perform resolve")?,
            Operation {
                typ: OperationType::Chargeback,
                amount: None,
                client,
                tx,
            } => handle_chargeback(&mut conn, client, tx)?,
            _ => return Err(anyhow!("malformed input CSV")),
        }
    }

    // Collect balances.
    let mut writer = csv::Writer::from_writer(output);
    let mut stmt = conn
        .prepare(
            "SELECT client, available, held, locked
             FROM balances
             /* Not needed per requirements but deterministic output is nice to have. */
             ORDER BY client ASC",
        )
        .context("internal error: cannot prepare statement")?;
    let balances = stmt
        .query_map(params![], |row| {
            let client: u16 = row.get(0)?;
            let available: i64 = row.get(1)?;
            let held: i64 = row.get(2)?;
            let locked = row.get(3)?;
            let total = available + held;
            Ok(Balance {
                client: ClientId(client),
                available: PreciseAmount(available),
                held: PreciseAmount(held),
                total: PreciseAmount(total),
                locked,
            })
        })
        .context("internal error: cannot query rows")?;

    // Write csv.
    writer
        .serialize(["client", "available", "held", "total", "locked"])
        .context("internal error: cannot open CSV writer")?;
    for balance in balances {
        let balance = balance.context("internal error: cannot read balance from database")?;
        writer
            .serialize(balance)
            .context("internal error: cannot write balance to CSV")?;
    }
    Ok(())
}

fn handle_withdrawal(
    conn: &mut Connection,
    amount: PreciseAmount,
    client: ClientId,
    txid: TxId,
) -> Result<()> {
    initialize_client_if_empty(conn, client)?;
    let tx = conn.transaction()?;
    if is_client_locked(&tx, client)? {
        return Err(anyhow!("cannot transact with locked client: {}", client.0));
    }

    let available = tx.query_row(
        "SELECT available FROM balances WHERE client = ?",
        params![client.0],
        |row| {
            let available: i64 = row.get(0).unwrap_or_else(|_| {
                panic!(
                    "internal error: client id {} is not present in the database",
                    client.0
                )
            });
            Ok(PreciseAmount(available))
        },
    )?;
    if available.0 < amount.0 {
        // Per requirements, ignore request. This could be done in SQL without the extra query, but
        // it is more robust if the logic is in Rust and we could do better error reporting and
        // logging.
        return Ok(());
    }
    tx.execute(
        "UPDATE balances
         SET available = available - ? 
         WHERE client = ?",
        params![amount.0, client.0],
    )?;
    tx.execute(
        "INSERT INTO transactions (type, state, id, client, amount)
         VALUES ('withdrawal', 'open', ?, ?, ?)",
        params![txid.0, client.0, amount.0],
    )?;
    tx.commit()?;

    Ok(())
}

fn handle_deposit(
    conn: &mut Connection,
    client: ClientId,
    amount: PreciseAmount,
    txid: TxId,
) -> Result<()> {
    initialize_client_if_empty(conn, client)?;
    let tx = conn.transaction()?;
    if is_client_locked(&tx, client)? {
        return Err(anyhow!("cannot transact with locked client: {}", client.0));
    }

    tx.execute(
        "UPDATE balances
         SET available = available + ?
         WHERE client = ?",
        params![amount.0, client.0],
    )?;
    tx.execute(
        "INSERT INTO transactions (type, state, id, client, amount)
         VALUES ('deposit', 'open', ?, ?, ?)",
        params![txid.0, client.0, amount.0],
    )?;
    tx.commit()?;

    Ok(())
}

fn handle_dispute(conn: &mut Connection, client: ClientId, txid: TxId) -> Result<()> {
    let tx = conn.transaction()?;
    let (state, amount, typ, txclient) = match tx.query_one(
        "SELECT state, amount, type, client FROM transactions WHERE id = ?",
        params![txid.0],
        |row| {
            let state: String = row.get(0)?;
            let amount: i64 = row.get(1)?;
            let typ: String = row.get(2)?;
            let txclient: u16 = row.get(3)?;
            Ok((
                TxState::from_db(&state),
                PreciseAmount(amount),
                typ,
                ClientId(txclient),
            ))
        },
    ) {
        Ok((state, amount, typ, client)) => (state, amount, typ, client),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // Per requirements, ignore.
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    if matches!(state, TxState::Pending) {
        return Err(anyhow!("cannot open dispute on pending transaction"));
    } else if matches!(state, TxState::Closed) {
        return Err(anyhow!("cannot open dispute on closed transaction"));
    } else if typ != "deposit" {
        return Err(anyhow!("cannot open dispute on non deposit"));
    } else if client != txclient {
        return Err(anyhow!(
            "cannot open dispute on transaction belonging to different client"
        ));
    }

    if is_client_locked(&tx, client)? {
        return Err(anyhow!("cannot transact with locked client: {}", client.0));
    }
    tx.execute(
        "UPDATE transactions
         SET state = ?
         WHERE id = ?",
        params![TxState::Pending.to_db(), txid.0],
    )?;
    tx.execute(
        "UPDATE balances
         SET available = available - :amount, held = held + :amount
         WHERE client = :client_id",
        named_params! {
            ":client_id": client.0,
            ":amount": amount.0,
        },
    )?;
    tx.commit()?;

    Ok(())
}

fn handle_resolve(conn: &mut Connection, client: ClientId, txid: TxId) -> Result<()> {
    let tx = conn.transaction()?;
    let (state, amount, txclient) = match tx.query_one(
        "SELECT state, amount, client FROM transactions WHERE id = ?",
        params![txid.0],
        |row| {
            let state: String = row.get(0)?;
            let amount: i64 = row.get(1)?;
            let client: u16 = row.get(2)?;
            Ok((
                TxState::from_db(&state),
                PreciseAmount(amount),
                ClientId(client),
            ))
        },
    ) {
        Ok((state, amount, txclient)) => (state, amount, txclient),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // Per requirements, ignore.
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    if !matches!(state, TxState::Pending) {
        // Per requirements, ignore.
        return Ok(());
    } else if client != txclient {
        return Err(anyhow!(
            "cannot open dispute on transaction belonging to different client"
        ));
    }

    if is_client_locked(&tx, client)? {
        return Err(anyhow!("cannot transact with locked client: {}", client.0));
    }
    tx.execute(
        "UPDATE transactions
         SET state = 'closed'
         WHERE id = ?",
        params![txid.0],
    )?;
    tx.execute(
        "UPDATE balances
         SET available = available + :amount, held = held - :amount
         WHERE client = :client_id",
        named_params! {
            ":client_id": client.0,
            ":amount": amount.0,
        },
    )?;
    tx.commit()?;

    Ok(())
}

fn handle_chargeback(conn: &mut Connection, client: ClientId, txid: TxId) -> Result<()> {
    let tx = conn.transaction()?;
    let (state, amount, txclient) = match tx.query_one(
        "SELECT state, amount, client FROM transactions WHERE id = ?",
        params![txid.0],
        |row| {
            let state: String = row.get(0)?;
            let amount: i64 = row.get(1)?;
            let client: u16 = row.get(2)?;
            Ok((
                TxState::from_db(&state),
                PreciseAmount(amount),
                ClientId(client),
            ))
        },
    ) {
        Ok((state, amount, txclient)) => (state, amount, txclient),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            // Per requirements, ignore.
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };
    if !matches!(state, TxState::Pending) {
        // Per requirements, ignore.
        return Ok(());
    } else if client != txclient {
        return Err(anyhow!(
            "cannot open dispute on transaction belonging to different client"
        ));
    }

    if is_client_locked(&tx, client)? {
        return Err(anyhow!("cannot transact with locked client: {}", client.0));
    }
    tx.execute(
        "UPDATE transactions
         SET state = 'closed'
         WHERE id = ?",
        params![txid.0],
    )?;
    tx.execute(
        "UPDATE balances
         SET held = held - :amount, locked = true
         WHERE client = :client_id",
        named_params! {
            ":client_id": client.0,
            ":amount": amount.0,
        },
    )?;
    tx.commit()?;

    Ok(())
}

fn is_client_locked(tx: &Transaction, client: ClientId) -> Result<bool> {
    let is_locked = tx.query_one(
        "SELECT locked FROM balances WHERE client = ?",
        params![client.0],
        |row| {
            let locked: bool = row.get(0)?;
            Ok(locked)
        },
    )?;
    Ok(is_locked)
}

fn initialize_client_if_empty(conn: &mut Connection, client_id: ClientId) -> Result<()> {
    conn.execute(
        "
        INSERT OR IGNORE INTO balances (client, available, held, locked)
        VALUES (?, 0, 0, 0)
        ",
        params![client_id.0],
    )?;

    Ok(())
}

fn main() -> Result<()> {
    const DB_PATH: &str = "./state.db";
    if env::args().len() != 2 {
        eprintln!("Expected one single argument: the path to the input CSV");
        process::exit(1);
    }
    let input = File::open(env::args().nth(1).unwrap())?;
    let output = io::stdout();

    exec(DB_PATH, input, output)?;
    Ok(())
}

const SCHEMA: &str = "
    CREATE TABLE transactions (
        id INTEGER PRIMARY KEY,
        type TEXT NOT NULL CHECK ( type IN ('deposit','withdrawal') ),
        /* Note: This should be a numerical column with an associated virtual text column for
         * debuggability. */
        state TEXT NOT NULL CHECK ( state IN ('open','pending','closed') ),
        client INTEGER NOT NULL,
        amount INTEGER,
        FOREIGN KEY(client) REFERENCES balances(client)
    ) STRICT;

    CREATE TABLE balances (
        client INTEGER PRIMARY KEY,
        available INTEGER NOT NULL,
        held INTEGER NOT NULL,
        locked INTEGER NOT NULL
    ) STRICT;
";

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use super::*;

    #[test]
    fn all() {
        // Tests are structured as: <test-case>.in and <expected-output>.out. When the output
        // starts with #<some error message> the test case expects an error instead.
        let input_files = glob::glob("tests/*.in").unwrap();
        for input_file in input_files {
            let input_file = input_file.unwrap();
            println!("Running: {}", input_file.display());
            let mut expected_file = input_file.clone();
            expected_file.set_extension("out");

            let temp_file = NamedTempFile::new().unwrap();
            let db_path = temp_file.path();
            let reader = File::open(&input_file).unwrap();
            let mut output_bytes: Vec<u8> = Vec::new();
            let result = exec(db_path, reader, &mut output_bytes);

            // Note: This could be more robust by reading the expected CSV into memory and
            // comparing both as lists of balances. Then, whitespace, formatting and ordering would
            // not matter.
            let expected = fs::read_to_string(expected_file).unwrap();
            if let Some(expected_err) = expected.strip_prefix("#") {
                let expected_err = expected_err.trim();
                let returned_err = format!("{:?}", result.unwrap_err());
                if !returned_err.contains(expected_err) {
                    println!(">>> Output:\n{}", returned_err);
                    println!(">>> Expected:\n{}", expected_err);
                    assert!(false);
                }
                continue;
            }
            result.unwrap();
            let output = String::from_utf8(output_bytes).unwrap();
            if output != expected {
                println!(">>> Test: '{}'", input_file.display());
                println!(">>> Output:\n{output}");
                println!(">>> Expected:\n{expected}");
                assert!(false);
            }
        }
    }

    #[test]
    fn precise_amount_parse() {
        fn test(input: &str, expected: i64) {
            let result = PreciseAmount::from_str(input).unwrap().0;
            if result != expected {
                println!("Input: {input}, output: {result}, expected: {expected}");
                assert!(false);
            }
        }
        test("1", 10000);
        test("1.", 10000);
        test("1.0", 10000);
        test("1.0000", 10000);

        test("1.1", 11000);
        test("1.01", 10100);
        test("1.001", 10010);
        test("1.0001", 10001);

        test("-1.0001", -10001);
    }

    #[test]
    fn precise_amount_serialize() {
        fn test(input: PreciseAmount, expected: &str) {
            let result = input.to_string();
            if result != expected {
                println!("Input: {}, output: {result}, expected: {expected}", input.0);
                assert!(false);
            }
        }
        test(PreciseAmount(10000), "1.0000");
        test(PreciseAmount(11000), "1.1000");
        test(PreciseAmount(10100), "1.0100");
        test(PreciseAmount(10010), "1.0010");
        test(PreciseAmount(10001), "1.0001");

        test(PreciseAmount(-10000), "-1.0000");
        test(PreciseAmount(-11000), "-1.1000");
        test(PreciseAmount(-10100), "-1.0100");
        test(PreciseAmount(-10010), "-1.0010");
        test(PreciseAmount(-10001), "-1.0001");

        test(PreciseAmount(-01000), "-0.1000");
        test(PreciseAmount(-00100), "-0.0100");
        test(PreciseAmount(-00010), "-0.0010");
        test(PreciseAmount(-00001), "-0.0001");
    }
}
