import asyncio
import os
import time
from typing import Optional
import httpx
from common.logger import setup_logger
from common.config import CLUSTER_AUTH_HEADER, CLUSTER_SECRET
from controlnode.metadata_store import MetadataStore

logger = setup_logger("HAManager")

class HAManager:
    """
    Manages High Availability (HA) active-standby replication and automatic failover (RNF2).
    """
    def __init__(self, metadata_store: MetadataStore):
        self.store = metadata_store
        self.role = os.getenv("DFSHA_ROLE", "primary").lower()  # "primary" or "standby"
        self.is_primary = (self.role == "primary")
        self.peer_url = os.getenv("DFSHA_PRIMARY_URL", "http://controlnode-primary:8000")
        self.sync_interval = float(os.getenv("DFSHA_HA_SYNC_INTERVAL", "3.0"))
        self.failover_threshold = int(os.getenv("DFSHA_HA_FAILOVER_THRESHOLD", "3"))
        self.failed_checks = 0
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self):
        self._running = True
        logger.info(f"HA Manager initialized with role: {self.role.upper()} (is_primary={self.is_primary})")
        if not self.is_primary:
            self._task = asyncio.create_task(self._standby_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()

    async def _standby_loop(self):
        """Continuously pulls state from Primary and monitors its health for failover."""
        logger.info(f"Standby node watching Primary at {self.peer_url}...")
        while self._running and not self.is_primary:
            try:
                headers = {CLUSTER_AUTH_HEADER: CLUSTER_SECRET}
                async with httpx.AsyncClient(timeout=3.0) as client:
                    resp = await client.get(f"{self.peer_url}/ha/snapshot", headers=headers)
                    if resp.status_code == 200:
                        snapshot = resp.json()
                        self.store.apply_state_snapshot(snapshot)
                        self.failed_checks = 0
                    else:
                        self.failed_checks += 1
                        logger.warning(f"Standby healthcheck warning: Primary returned status {resp.status_code}")
            except Exception as e:
                self.failed_checks += 1
                logger.warning(f"Standby failed to reach Primary ({self.failed_checks}/{self.failover_threshold}): {e}")

            if self.failed_checks >= self.failover_threshold:
                logger.critical("!!! PRIMARY CONTROLNODE DOWN: INITIATING AUTOMATIC FAILOVER !!!")
                self.promote_to_primary()
                break

            await asyncio.sleep(self.sync_interval)

    def promote_to_primary(self):
        """Promotes this standby node to active Primary."""
        self.is_primary = True
        self.role = "primary"
        logger.info(">>> THIS NODE IS NOW THE ACTIVE PRIMARY CONTROLNODE <<<")

    async def sync_mutation_to_standby(self, standby_url: str):
        """Pushes state to standby after write operation."""
        if not standby_url or not self.is_primary:
            return
        try:
            snapshot = self.store.get_state_snapshot()
            headers = {CLUSTER_AUTH_HEADER: CLUSTER_SECRET}
            async with httpx.AsyncClient(timeout=3.0) as client:
                await client.post(f"{standby_url}/ha/sync", json=snapshot, headers=headers)
        except Exception as e:
            logger.debug(f"Async replication to standby failed: {e}")
