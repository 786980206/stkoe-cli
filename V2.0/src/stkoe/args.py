"""命令行 flag 解析：``--key value`` / ``--key=value`` / ``--flag`` 序列

键名保持用户输入的形态（含连字符，如 ``grpc-host``），不做任何转换。
供 CLI 与 gRPC Execute 的 ``config set`` 等命令共用。
"""


def parse_flags(raw: list[str]) -> dict:
    """解析 ``--key value`` / ``--key=value`` / ``--flag`` 序列为字典"""
    kv: dict = {}
    i = 0
    while i < len(raw):
        a = raw[i]
        if a.startswith("--"):
            key = a[2:]
            if "=" in key:
                k, v = key.split("=", 1)
                kv[k] = v
            elif i + 1 < len(raw) and not raw[i + 1].startswith("--"):
                kv[key] = raw[i + 1]
                i += 1
            else:
                kv[key] = True
        i += 1
    return kv
