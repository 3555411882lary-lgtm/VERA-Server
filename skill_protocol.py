"""Quaternion skill transport. No guessed rest-pose removal or Euler round-trip."""
import json
import math
from pathlib import Path


def load_quaternion_skills(directory):
    directory = Path(directory)
    bones = json.loads((directory / 'body_model/model.json').read_text(encoding='utf-8-sig'))['bones']
    source = json.loads((directory / 'skills_quat.json').read_text(encoding='utf-8-sig'))
    result = {}
    for name, clip in source.items():
        frames = []
        for i, row in enumerate(clip['frames']):
            if len(row) != len(bones):
                raise ValueError(f'{name}: bone count mismatch')
            entries = {}
            for bone, values in zip(bones, row):
                if len(values) != 4 or not all(math.isfinite(v) for v in values):
                    raise ValueError(f'{name}/{bone}: invalid quaternion')
                norm = math.sqrt(sum(v*v for v in values))
                if norm < 1e-8:
                    raise ValueError(f'{name}/{bone}: zero quaternion')
                entries[bone] = dict(zip(('qx','qy','qz','qw'), (v/norm for v in values)))
            frames.append({'t': i / 20.0, 'bones': entries})
        if frames:
            result[name] = frames
    return result
