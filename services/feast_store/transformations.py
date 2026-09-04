"""Feature transformation boundary; formulas will be implemented later."""


def compute_features(rows):
    """Transform resampled market rows into the versioned feature schema."""
    raise NotImplementedError


def build_feature_frame(resampled_rows):
    """Build one venue-agnostic feature frame for offline and online use."""
    raise NotImplementedError
