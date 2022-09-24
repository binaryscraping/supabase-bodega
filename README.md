# supabase-bodega

A Bodega storage implementation backed by Supabase.

## Getting Started

### Creating PostgreSQL schema

```sql
create table tests (
    key text primary key not null,
    data text not null,
    created_at timestamp with time zone default timezone('utc'::text, now()) not null,
    updated_at timestamp with time zone default timezone('utc'::text, now()) not null
);

create trigger handle_updated_at
    before update on tests for each row
    execute procedure moddatetime (updated_at);
```
