import re
import sqlparse

def split_plsql_into_blocks(plsql: str, max_chunk_size=1200):
    """
    Splits PL/SQL code into logical blocks:
    - DECLARE...BEGIN...END;
    - CREATE [OR REPLACE] FUNCTION/PROCEDURE/TRIGGER...END;
    - Standalone statements
    """
    # Normalize line endings
    code = plsql.replace('\r\n', '\n').replace('\r', '\n')
    statements = sqlparse.split(code)
    blocks = []
    buffer = ""
    in_block = False
    block_start_pattern = re.compile(r'^\s*(CREATE|DECLARE|BEGIN)', re.IGNORECASE)
    block_end_pattern = re.compile(r'END\s*;|END\s*[\w$]*\s*;', re.IGNORECASE)
    for stmt in statements:
        stripped = stmt.strip()
        # Start of a block
        if block_start_pattern.match(stripped):
            if buffer:
                blocks.append(buffer.strip())
                buffer = ""
            in_block = True
        buffer += (stmt if buffer == "" else "\n" + stmt)
        # End of a block
        if in_block and block_end_pattern.search(stripped):
            blocks.append(buffer.strip())
            buffer = ""
            in_block = False
    if buffer.strip():
        blocks.append(buffer.strip())

    # Fallback: if still too big, chunk by chars
    final_blocks = []
    for blk in blocks:
        if len(blk) > max_chunk_size:
            # Split by ';' but keep context
            stmts = blk.split(';')
            temp = ""
            for s in stmts:
                temp += s + ';'
                if len(temp) >= max_chunk_size:
                    final_blocks.append(temp.strip())
                    temp = ""
            if temp.strip():
                final_blocks.append(temp.strip())
        else:
            final_blocks.append(blk)
    return final_blocks