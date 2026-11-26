# Copyright VecturaDB contributors. Licensed under the Apache License, Version 2.0.
from chromadb.auth.utils import (  # noqa: F401
    _singleton_tenant_database_if_applicable as _singleton_tenant_database_if_applicable,
)
from chromadb.auth.utils import (  # noqa: F401
    maybe_set_tenant_and_database as maybe_set_tenant_and_database,
)
from chromadb.auth.utils import (  # type: ignore[attr-defined]  # noqa: F401
    ChromaAuthError,
)
from chromadb.auth.utils import (  # type: ignore[attr-defined]  # noqa: F401
    DEFAULT_DATABASE,
)
from chromadb.auth.utils import (  # type: ignore[attr-defined]  # noqa: F401
    DEFAULT_TENANT,
)
from chromadb.auth.utils import Optional  # type: ignore[attr-defined]  # noqa: F401
from chromadb.auth.utils import Tuple  # type: ignore[attr-defined]  # noqa: F401
from chromadb.auth.utils import UserIdentity  # type: ignore[attr-defined]  # noqa: F401
