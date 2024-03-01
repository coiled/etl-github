import dask_deltatable as ddt
import streamlit as st

from preprocess import OUTDIR


def get_data(variable):
    df = ddt.read_deltalake(OUTDIR / "dask" / variable).compute()
    df = df.drop(columns="__index_level_0__")
    df["repo"] = "https://github.com/" + df["repo"]
    df["username"] = "https://github.com/" + df["username"]
    # Streamlit doesn't like pyarrow strings
    # (xref https://github.com/streamlit/streamlit/issues/6334)
    str_cols = df.select_dtypes(include="string").columns
    df = df.astype({c: object for c in str_cols})
    return df


st.markdown("""
## Dask mentions on GitHub
### Commits
""")
df = get_data("commits")
st.dataframe(
    df,
    column_config={
        "repo": st.column_config.LinkColumn("repo"),
        "username": st.column_config.LinkColumn("user"),
    },
    hide_index=True,
)
st.markdown("""
### Comments
""")
df = get_data("comments")
st.dataframe(
    df,
    column_config={
        "repo": st.column_config.LinkColumn("repo"),
        "username": st.column_config.LinkColumn("user"),
    },
    hide_index=True,
)
