import argparse
import sys
import uuid
import logging
from sqlalchemy import select

try:
    from sir import config
    from sir.indexing import live_index
    from sir.schema import SCHEMA
    from sir.util import db_session
    from mbdata.models import Artist, ArtistCreditName, Track, Medium
except ImportError:
    print("Error: Could not import 'sir'. Make sure you run this script from the 'sir' repository root and set PYTHONPATH=.")
    sys.exit(1)

logger = logging.getLogger("sir")
logging.basicConfig(level=logging.INFO)

def main():
    parser = argparse.ArgumentParser(description="Reindex all content (recordings, releases, release groups) related to an artist by GID.")
    parser.add_argument("gid", type=str, help="Artist GID (UUID)")
    args = parser.parse_args()

    # Load configuration
    try:
        config.read_config()
    except Exception as e:
        logger.error(f"Error reading configuration: {e}")
        sys.exit(1)

    try:
        artist_gid = uuid.UUID(args.gid)
    except ValueError:
        logger.error(f"Invalid UUID: {args.gid}")
        sys.exit(1)

    # Get models from SCHEMA to ensure we correspond to what sir expects
    try:
        RecordingModel = SCHEMA["recording"].model
        ReleaseModel = SCHEMA["release"].model
        ReleaseGroupModel = SCHEMA["release-group"].model
    except KeyError:
        logger.error("Recording, Release, or ReleaseGroup schema not found.")
        sys.exit(1)

    session_factory = db_session()
    with session_factory() as session:
        # Resolve GID to Artist ID
        try:
            stmt_artist = select(Artist.id).where(Artist.gid == artist_gid)
            artist_result = session.execute(stmt_artist).scalar_one_or_none()
        except Exception as e:
            logger.error(f"Database query failed for artist: {e}")
            sys.exit(1)

        if artist_result is None:
            logger.error(f"Artist with GID {args.gid} not found.")
            sys.exit(1)

        artist_id = artist_result
        logger.info(f"Found local artist ID: {artist_id} for GID: {artist_gid}")

        # Find all ArtistCredit IDs this artist is part of
        try:
            stmt_acns = select(ArtistCreditName.artist_credit_id).where(ArtistCreditName.artist_id == artist_id)
            acn_results = session.execute(stmt_acns).scalars().all()
            
            # Using list for SQLAlchemy .in_() clause
            artist_credit_ids = list(set(acn_results))
        except Exception as e:
            logger.error(f"Database query failed for artist credits: {e}")
            sys.exit(1)

        if not artist_credit_ids:
            logger.info("Artist has no credits. Nothing to index.")
            return

        logger.info(f"Artist belongs to {len(artist_credit_ids)} different credited entities (artist credits).")

        entities_to_index = {}

        # 1. Recordings
        try:
            stmt_recs = select(RecordingModel.id).where(RecordingModel.artist_credit_id.in_(artist_credit_ids))
            recording_ids = list(set(session.execute(stmt_recs).scalars().all()))
            if recording_ids:
                entities_to_index["recording"] = recording_ids
            logger.info(f"Found {len(recording_ids)} recordings to reindex.")
        except Exception as e:
            logger.error(f"Failed to fetch recordings: {e}")

        # 2. Releases
        try:
            # Releases by release artist_credit_id
            stmt_rels = select(ReleaseModel.id).where(ReleaseModel.artist_credit_id.in_(artist_credit_ids))
            release_ids = set(session.execute(stmt_rels).scalars().all())

            # Releases by track artist_credit_id
            stmt_rel_from_tracks = select(ReleaseModel.id).\
                join(Medium, Medium.release_id == ReleaseModel.id).\
                join(Track, Track.medium_id == Medium.id).\
                where(Track.artist_credit_id.in_(artist_credit_ids))
            
            release_from_track_ids = set(session.execute(stmt_rel_from_tracks).scalars().all())
            release_ids.update(release_from_track_ids)
            
            release_ids_list = list(release_ids)
            if release_ids_list:
                entities_to_index["release"] = release_ids_list
            logger.info(f"Found {len(release_ids_list)} releases to reindex.")
        except Exception as e:
            logger.error(f"Failed to fetch releases: {e}")
            release_ids_list = []

        # 3. Release Groups
        try:
            stmt_rgs = select(ReleaseGroupModel.id).where(ReleaseGroupModel.artist_credit_id.in_(artist_credit_ids))
            rg_ids = set(session.execute(stmt_rgs).scalars().all())

            if release_ids_list:
                stmt_rgs_from_releases = select(ReleaseGroupModel.id).\
                    join(ReleaseModel, ReleaseModel.release_group_id == ReleaseGroupModel.id).\
                    where(ReleaseModel.id.in_(release_ids_list))
                rgs_from_releases = set(session.execute(stmt_rgs_from_releases).scalars().all())
                rg_ids.update(rgs_from_releases)
                
            rg_ids_list = list(rg_ids)
            if rg_ids_list:
                entities_to_index["release-group"] = rg_ids_list
            logger.info(f"Found {len(rg_ids_list)} release groups to reindex.")
        except Exception as e:
            logger.error(f"Failed to fetch release groups: {e}")

        total_entities = sum(len(v) for v in entities_to_index.values())
        if total_entities == 0:
            logger.info("No entities found to reindex.")
            return

        logger.info(f"Starting live reindex for {total_entities} entities...")

    # Trigger live index
    try:
        live_index(entities_to_index)
        logger.info("Reindexing complete.")
    except Exception as e:
        logger.error(f"Reindexing failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
