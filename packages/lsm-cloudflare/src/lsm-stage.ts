/** Stage names that run only under Alchemy's local providers: `dev`, `test`, `local`. */
const LOCAL_LSM_STAGE_PATTERN = /^(?:dev|test|local)(?:-[a-z0-9-]+)?$/;

/**
 * Whether an Alchemy stage runs only under Alchemy's local providers: `dev`, `test`, `local`, and
 * `dev-*`, `test-*`, `local-*`. Every other stage (`production`, `preview-1`, ...) is live.
 *
 * **Details**
 *
 * Local stages keep their Alchemy state on this machine and export spans before each response;
 * live stages keep their state in the account's Cloudflare state store and export spans after the
 * response.
 */
export const isLocalLsmStage = (stage: string): boolean => LOCAL_LSM_STAGE_PATTERN.test(stage);
