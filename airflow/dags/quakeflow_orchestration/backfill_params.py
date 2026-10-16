"""Check what a person typed into the backfill form before anything is downloaded.

Standard library only, because it runs inside the Airflow image.
"""

from datetime import date

MAX_DAYS = 366
EARLIEST = date(1900, 1, 1)


def validate_backfill(start: object, end: object, min_magnitude: object, today: date | None = None) -> dict:
    """Return the normalized parameters, or raise ValueError naming every problem found."""
    today = today or date.today()
    problems: list[str] = []
    first = last = None
    for label, value in (("start", start), ("end", end)):
        try:
            parsed = date.fromisoformat(str(value))
        except ValueError:
            problems.append(f"{label} must be a date as YYYY-MM-DD, got {value!r}")
            continue
        if label == "start":
            first = parsed
        else:
            last = parsed
    if first and last:
        if first >= last:
            problems.append("start must be before end (end is the day after the last day wanted)")
        elif (last - first).days > MAX_DAYS:
            problems.append(f"the range is {(last - first).days} days, the limit is {MAX_DAYS}")
        if first < EARLIEST:
            problems.append(f"start is before {EARLIEST}")
        if last > today:
            problems.append(f"end {last} is in the future")

    magnitude = None
    if min_magnitude not in (None, ""):
        if isinstance(min_magnitude, bool) or not isinstance(min_magnitude, int | float | str):
            problems.append(f"min_magnitude must be a number, got {min_magnitude!r}")
        else:
            try:
                magnitude = float(min_magnitude)
            except ValueError:
                problems.append(f"min_magnitude must be a number, got {min_magnitude!r}")
            else:
                if not -2.0 <= magnitude <= 10.0:
                    problems.append(f"min_magnitude {magnitude} is outside -2 to 10")
    if problems:
        raise ValueError("; ".join(problems))
    return {"start": first.isoformat(), "end": last.isoformat(), "min_magnitude": magnitude}
