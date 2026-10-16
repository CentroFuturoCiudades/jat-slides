import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import matplotlib.transforms as mtrans
import numpy as np
import pandas as pd

CVE_MET = "19.1.01"
REMOTENESS_LABELS = ["Central", "Intermedia", "Distante", "Periurbana"]
START_YEAR = 1990

df = (
    pd.read_csv(f"./data/input/radial_f/radial_functions_{CVE_MET}_rho.csv")
    .assign(
        **{
            f"remoteness_{year}": lambda df, year=year: (
                df["r"] / np.sqrt(df[f"ring_pop_{year}"].sum())
            )
            for year in range(1990, 2021, 10)
        }
    )
    .assign(r=lambda df: df["r"].div(1000))
    .set_index("r")
)

df_brackets = pd.read_csv("./data/input/remoteness_brackets.csv")

x = df.index
y = df["ring_pop_2020"] - df[f"ring_pop_{START_YEAR}"]

cum_pop = df["ring_pop_2020"].cumsum()
top_pop = cum_pop.quantile(0.9)
xmax = cum_pop[cum_pop < top_pop].index[-1]

fig, ax = plt.subplots(figsize=(10, 5))
y.plot(ax=ax, c="k", alpha=0.7)
ax.fill_between(x, y, where=y > 0, interpolate=True, color="#1E90FF", alpha=0.7)
ax.fill_between(x, y, where=y < 0, interpolate=True, color="#FF6347", alpha=0.7)
ax.set_xlim(0, xmax)
ax.yaxis.set_major_formatter(mtick.StrMethodFormatter("{x:,.0f}"))
ax.set_xlabel("Distancia al centro (km)")
ax.set_ylabel("Cambio en la población")

mix_transform = mtrans.blended_transform_factory(ax.transData, ax.transAxes)

split_coods = [0]
for col in ("r_3", "r_5", "r_9.3"):
    split_coord = df_brackets.loc[df_brackets["CVE_MET"] == CVE_MET, col].iloc[0]
    ax.axvline(
        split_coord,
        ls="--",
        c="k",
        alpha=0.5,
    )
    split_coods.append(split_coord)
split_coods = np.array(split_coods)

for i, label in enumerate(REMOTENESS_LABELS):
    left = split_coods[i]
    right = split_coods[i + 1] if i < 3 else xmax
    x = (left + right) / 2
    ax.annotate(
        label,
        xy=(x, 1.01),
        ha="center",
        xycoords=mix_transform,
        rotation=0,
        fontsize=12,
    )
fig.savefig("temp.jpg", bbox_inches="tight", dpi=300)


df_diff = df.assign(
    remoteness_cat=lambda df: (
        pd.cut(df.index, bins=np.append(split_coods, np.inf), labels=REMOTENESS_LABELS)  # ty: ignore[no-matching-overload]
        .assign(pop_diff=lambda df: df["ring_pop_2020"] - df[f"ring_pop_{START_YEAR}"])[
            ["pop_diff", "remoteness_cat"]
        ]
        .groupby("remoteness_cat")
        .sum()
    )
)
