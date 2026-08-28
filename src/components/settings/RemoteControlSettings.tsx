import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { copyText } from "@/lib/clipboard";
import { settingsApi } from "@/lib/api";
import type {
  RemoteControlSettings as RemoteSettings,
  RemoteControlStatus,
} from "@/types";

const STATUS_STYLES: Record<RemoteControlStatus, string> = {
  disabled: "bg-muted-foreground/45",
  connecting: "bg-amber-500 animate-pulse",
  online: "bg-emerald-500",
  error: "bg-red-500",
};

export function RemoteControlSettings() {
  const { t } = useTranslation();
  const [settings, setSettings] = useState<RemoteSettings | null>(null);
  const [enabled, setEnabled] = useState(false);
  const [relayUrl, setRelayUrl] = useState("");
  const [accessKey, setAccessKey] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const loadSettings = useCallback(async () => {
    try {
      const next = await settingsApi.getRemoteControlSettings();
      setSettings(next);
      setEnabled(next.enabled);
      setRelayUrl(next.relayUrl);
      setError(null);
    } catch (loadError) {
      setError(errorMessage(loadError));
    }
  }, []);

  useEffect(() => {
    void loadSettings();
  }, [loadSettings]);

  useEffect(() => {
    if (!settings?.enabled) return;
    const timer = window.setInterval(() => void loadSettings(), 3_000);
    return () => window.clearInterval(timer);
  }, [loadSettings, settings?.enabled]);

  async function generateKey() {
    try {
      const key = await settingsApi.generateRemoteAccessKey();
      setAccessKey(key);
      await copyText(key);
      toast.success(
        t("settings.remote.keyGenerated", {
          defaultValue: "新 Access Key 已生成并复制，请保存后在手机网页输入。",
        }),
      );
    } catch (generateError) {
      setError(errorMessage(generateError));
    }
  }

  async function copyKey() {
    try {
      const key = accessKey || (await settingsApi.getRemoteAccessKey());
      await copyText(key);
      toast.success(
        t("settings.remote.keyCopied", { defaultValue: "Access Key 已复制" }),
      );
    } catch (copyError) {
      setError(errorMessage(copyError));
    }
  }

  async function save() {
    setBusy(true);
    setError(null);
    try {
      const next = await settingsApi.saveRemoteControlSettings({
        enabled,
        relayUrl: relayUrl.trim(),
        accessKey: accessKey.trim() || undefined,
      });
      setSettings(next);
      setEnabled(next.enabled);
      setRelayUrl(next.relayUrl);
      setAccessKey("");
      toast.success(
        t("settings.remote.saved", { defaultValue: "Remote 设置已保存" }),
      );
    } catch (saveError) {
      setError(errorMessage(saveError));
    } finally {
      setBusy(false);
    }
  }

  const status = settings?.status ?? "disabled";
  const statusLabel = t(`settings.remote.status.${status}`, {
    defaultValue: {
      disabled: "未启用",
      connecting: "正在连接",
      online: "电脑在线",
      error: "启动失败",
    }[status],
  });

  return (
    <section className="space-y-4">
      <div className="flex items-end justify-between gap-4 border-b border-border/40 pb-3">
        <div className="min-w-0">
          <h3 className="text-sm font-medium">
            {t("settings.remote.title", { defaultValue: "Codex Remote" })}
          </h3>
          <p className="mt-1 max-w-[62ch] text-xs leading-relaxed text-muted-foreground">
            {t("settings.remote.description", {
              defaultValue:
                "让手机网页查看并操作这台电脑上的 Codex 任务。对话正文只经过 Relay 转发，不在 Relay 持久化。",
            })}
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-2 rounded-full border border-border/60 px-3 py-1.5 text-xs text-muted-foreground">
          <span className={`h-2 w-2 rounded-full ${STATUS_STYLES[status]}`} />
          {statusLabel}
        </div>
      </div>

      {settings === null && !error ? (
        <div className="space-y-3" aria-label="正在加载 Remote 设置">
          <div className="h-9 animate-pulse rounded-md bg-muted" />
          <div className="h-9 animate-pulse rounded-md bg-muted" />
        </div>
      ) : (
        <div className="space-y-4">
          <div className="flex items-center justify-between gap-6 rounded-xl border border-border/60 bg-muted/20 px-4 py-3">
            <div>
              <Label htmlFor="remote-enabled" className="text-sm font-medium">
                {t("settings.remote.enable", {
                  defaultValue: "启用 Remote Agent",
                })}
              </Label>
              <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
                {t("settings.remote.enableHint", {
                  defaultValue:
                    "CC Switch 启动后自动连接 Relay；电脑离线时手机仅显示缓存。",
                })}
              </p>
            </div>
            <Switch
              id="remote-enabled"
              checked={enabled}
              onCheckedChange={setEnabled}
              disabled={busy}
            />
          </div>

          <div className="grid gap-2">
            <Label htmlFor="remote-relay-url">
              {t("settings.remote.relayUrl", {
                defaultValue: "Relay WebSocket URL",
              })}
            </Label>
            <Input
              id="remote-relay-url"
              value={relayUrl}
              onChange={(event) => setRelayUrl(event.target.value)}
              placeholder="wss://remote.example.com/ws/agent"
              disabled={busy}
            />
            <p className="text-xs text-muted-foreground">
              {t("settings.remote.relayHint", {
                defaultValue:
                  "公网地址必须使用 wss://；只有本机调试允许 ws://。",
              })}
            </p>
          </div>

          <div className="grid gap-2">
            <Label htmlFor="remote-access-key">Access Key</Label>
            <div className="grid grid-cols-[minmax(0,1fr)_auto_auto] gap-2">
              <Input
                id="remote-access-key"
                type="password"
                value={accessKey}
                onChange={(event) => setAccessKey(event.target.value)}
                placeholder={
                  settings?.hasAccessKey
                    ? t("settings.remote.keySaved", {
                        defaultValue: "已配置；留空可保留现有 Key",
                      })
                    : t("settings.remote.keyMissing", {
                        defaultValue: "请生成或输入 32–256 字符的 Key",
                      })
                }
                disabled={busy}
              />
              <Button
                type="button"
                variant="outline"
                onClick={generateKey}
                disabled={busy}
              >
                {t("settings.remote.generate", { defaultValue: "生成" })}
              </Button>
              <Button
                type="button"
                variant="outline"
                onClick={copyKey}
                disabled={busy || (!accessKey && !settings?.hasAccessKey)}
              >
                {t("common.copy", { defaultValue: "复制" })}
              </Button>
            </div>
            <p className="text-xs text-muted-foreground">
              {t("settings.remote.keyHint", {
                defaultValue:
                  "一个 Access Key 对应这台电脑。已保存的 Key 可按需重复复制；需要更换时生成新 Key。",
              })}
            </p>
          </div>

          {error || settings?.lastError ? (
            <div className="rounded-lg border border-red-500/25 bg-red-500/5 px-3 py-2 text-xs leading-relaxed text-red-600 dark:text-red-400">
              {error || settings?.lastError}
            </div>
          ) : null}

          <div className="flex justify-end">
            <Button
              type="button"
              onClick={save}
              disabled={busy}
              className="active:scale-[0.98]"
            >
              {busy
                ? t("common.saving", { defaultValue: "保存中…" })
                : t("common.save", { defaultValue: "保存" })}
            </Button>
          </div>
        </div>
      )}
    </section>
  );
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error || "操作失败");
}
