use barrel::{types, Migration, backend::Pg};

fn bigint() -> types::Type {
    barrel::types::custom("bigint")
}

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("torrent", |t| {
        t.add_column("id", types::serial().nullable(false));
        t.add_column("info_hash", types::text().nullable(false));
        t.add_column("info_data", types::binary().nullable(false));
        t.add_column("annotations", types::json().nullable(false));
        t.add_column("state", types::varchar(32).nullable(false));
        t.add_column("bitfield", types::binary().nullable(true));
    });

    m.create_table("statistics", |t| {
        t.add_column("info_hash", types::text().nullable(false));
        t.add_column("session_id", types::text().nullable(false));
        t.set_primary_key(&["info_hash", "session_id"]);
        // will always be 1 until aggregated.
        t.add_column("conn_count", bigint().nullable(false));
        t.add_column("rx_data", bigint().nullable(false));
        t.add_column("tx_data", bigint().nullable(false));
    });

    m.make::<Pg>()
}
