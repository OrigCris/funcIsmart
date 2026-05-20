-- =============================================================================
-- Setup único: rode este script no SQL Editor do Supabase (uma única vez).
--
-- Cria a função public.ensure_columns(table_name, columns) que adiciona
-- colunas em lote a uma tabela do schema public, ignorando as que já existem.
--
-- Uso esperado pelo Python (sql_to_supabase.job.ensure_supabase_columns):
--   supabase.rpc('ensure_columns', {
--       'p_table': 'minha_tabela',
--       'p_columns': [{"name": "col_nova", "type": "text"}, ...]
--   }).execute()
-- =============================================================================

create or replace function public.ensure_columns(
    p_table   text,
    p_columns jsonb
)
returns void
language plpgsql
security definer
set search_path = public
as $$
declare
    col jsonb;
    col_name text;
    col_type text;
begin
    if p_columns is null or jsonb_typeof(p_columns) <> 'array' then
        return;
    end if;

    for col in select * from jsonb_array_elements(p_columns)
    loop
        col_name := col->>'name';
        col_type := col->>'type';

        if col_name is null or col_type is null then
            continue;
        end if;

        execute format(
            'alter table public.%I add column if not exists %I %s',
            p_table, col_name, col_type
        );
    end loop;
end;
$$;

-- Permite chamar via service_role (que é a chave usada pelo função Python).
grant execute on function public.ensure_columns(text, jsonb) to service_role;
