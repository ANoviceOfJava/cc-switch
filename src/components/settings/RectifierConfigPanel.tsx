import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { toast } from "sonner";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  settingsApi,
  type RectifierConfig,
  type VisionBridgeConfig,
  type OptimizerConfig,
} from "@/lib/api/settings";

export function RectifierConfigPanel() {
  const { t } = useTranslation();
  const defaultVisionBridge: VisionBridgeConfig = {
    enabled: false,
    apiUrl: "https://open.bigmodel.cn/api/paas/v4/chat/completions",
    apiKey: "",
    model: "glm-4v-flash",
    prompt: "",
    timeoutSeconds: 60,
  };
  const [config, setConfig] = useState<RectifierConfig>({
    enabled: true,
    requestThinkingSignature: true,
    requestThinkingBudget: true,
    requestMediaFallback: true,
    requestMediaHeuristic: true,
    visionBridge: defaultVisionBridge,
  });
  const [optimizerConfig, setOptimizerConfig] = useState<OptimizerConfig>({
    enabled: false,
    thinkingOptimizer: true,
    cacheInjection: true,
  });
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    settingsApi
      .getRectifierConfig()
      .then((loaded) =>
        setConfig({
          ...loaded,
          visionBridge: { ...defaultVisionBridge, ...loaded.visionBridge },
        }),
      )
      .catch((e) => console.error("Failed to load rectifier config:", e))
      .finally(() => setIsLoading(false));
    settingsApi
      .getOptimizerConfig()
      .then(setOptimizerConfig)
      .catch((e) => console.error("Failed to load optimizer config:", e));
  }, []);

  const handleChange = async (updates: Partial<RectifierConfig>) => {
    const newConfig = { ...config, ...updates };
    setConfig(newConfig);
    try {
      await settingsApi.setRectifierConfig(newConfig);
    } catch (e) {
      console.error("Failed to save rectifier config:", e);
      toast.error(String(e));
      setConfig(config);
    }
  };

  const handleVisionBridgeChange = async (
    updates: Partial<VisionBridgeConfig>,
  ) => {
    const newConfig = {
      ...config,
      visionBridge: { ...config.visionBridge, ...updates },
    };
    setConfig(newConfig);
    try {
      await settingsApi.setRectifierConfig(newConfig);
    } catch (e) {
      console.error("Failed to save vision bridge config:", e);
      toast.error(String(e));
      setConfig(config);
    }
  };

  const handleOptimizerChange = async (updates: Partial<OptimizerConfig>) => {
    const newConfig = { ...optimizerConfig, ...updates };
    setOptimizerConfig(newConfig);
    try {
      await settingsApi.setOptimizerConfig(newConfig);
    } catch (e) {
      console.error("Failed to save optimizer config:", e);
      toast.error(String(e));
      setOptimizerConfig(optimizerConfig);
    }
  };

  if (isLoading) return null;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div className="space-y-0.5">
          <Label>{t("settings.advanced.rectifier.enabled")}</Label>
          <p className="text-xs text-muted-foreground">
            {t("settings.advanced.rectifier.enabledDescription")}
          </p>
        </div>
        <Switch
          checked={config.enabled}
          onCheckedChange={(checked) => handleChange({ enabled: checked })}
        />
      </div>

      <div className="space-y-4">
        <h4 className="text-sm font-medium text-muted-foreground">
          {t("settings.advanced.rectifier.requestGroup")}
        </h4>
        <div className="flex items-center justify-between pl-4">
          <div className="space-y-0.5">
            <Label>{t("settings.advanced.rectifier.thinkingSignature")}</Label>
            <p className="text-xs text-muted-foreground">
              {t("settings.advanced.rectifier.thinkingSignatureDescription")}
            </p>
          </div>
          <Switch
            checked={config.requestThinkingSignature}
            disabled={!config.enabled}
            onCheckedChange={(checked) =>
              handleChange({ requestThinkingSignature: checked })
            }
          />
        </div>
        <div className="flex items-center justify-between pl-4">
          <div className="space-y-0.5">
            <Label>{t("settings.advanced.rectifier.thinkingBudget")}</Label>
            <p className="text-xs text-muted-foreground">
              {t("settings.advanced.rectifier.thinkingBudgetDescription")}
            </p>
          </div>
          <Switch
            checked={config.requestThinkingBudget}
            disabled={!config.enabled}
            onCheckedChange={(checked) =>
              handleChange({ requestThinkingBudget: checked })
            }
          />
        </div>
        <div className="flex items-center justify-between pl-4">
          <div className="space-y-0.5">
            <Label>{t("settings.advanced.rectifier.mediaFallback")}</Label>
            <p className="text-xs text-muted-foreground">
              {t("settings.advanced.rectifier.mediaFallbackDescription")}
            </p>
          </div>
          <Switch
            checked={config.requestMediaFallback}
            disabled={!config.enabled}
            onCheckedChange={(checked) =>
              handleChange({ requestMediaFallback: checked })
            }
          />
        </div>
        <div className="flex items-center justify-between pl-8">
          <div className="space-y-0.5">
            <Label>{t("settings.advanced.rectifier.mediaHeuristic")}</Label>
            <p className="text-xs text-muted-foreground">
              {t("settings.advanced.rectifier.mediaHeuristicDescription")}
            </p>
          </div>
          <Switch
            checked={config.requestMediaHeuristic}
            disabled={!config.enabled || !config.requestMediaFallback}
            onCheckedChange={(checked) =>
              handleChange({ requestMediaHeuristic: checked })
            }
          />
        </div>
        <div className="rounded-lg border border-border/60 p-4 pl-4 space-y-4">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5">
              <Label>{t("settings.advanced.rectifier.visionBridge")}</Label>
              <p className="text-xs text-muted-foreground">
                {t("settings.advanced.rectifier.visionBridgeDescription")}
              </p>
            </div>
            <Switch
              checked={config.visionBridge.enabled}
              disabled={!config.enabled || !config.requestMediaFallback}
              onCheckedChange={(checked) =>
                handleVisionBridgeChange({ enabled: checked })
              }
            />
          </div>
          {config.visionBridge.enabled && (
            <div className="space-y-4 pl-4">
              <div className="grid gap-1.5">
                <Label htmlFor="vision-api-url">
                  {t("settings.advanced.rectifier.visionApiUrl")}
                </Label>
                <Input
                  id="vision-api-url"
                  value={config.visionBridge.apiUrl}
                  onChange={(e) =>
                    handleVisionBridgeChange({ apiUrl: e.target.value })
                  }
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="vision-api-key">
                  {t("settings.advanced.rectifier.visionApiKey")}
                </Label>
                <Input
                  id="vision-api-key"
                  type="password"
                  value={config.visionBridge.apiKey}
                  onChange={(e) =>
                    handleVisionBridgeChange({ apiKey: e.target.value })
                  }
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="vision-model">
                  {t("settings.advanced.rectifier.visionModel")}
                </Label>
                <Input
                  id="vision-model"
                  value={config.visionBridge.model}
                  onChange={(e) =>
                    handleVisionBridgeChange({ model: e.target.value })
                  }
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="vision-timeout">
                  {t("settings.advanced.rectifier.visionTimeout")}
                </Label>
                <Input
                  id="vision-timeout"
                  type="number"
                  min={5}
                  value={config.visionBridge.timeoutSeconds}
                  onChange={(e) =>
                    handleVisionBridgeChange({
                      timeoutSeconds: Number(e.target.value) || 60,
                    })
                  }
                />
              </div>
              <div className="grid gap-1.5">
                <Label htmlFor="vision-prompt">
                  {t("settings.advanced.rectifier.visionPrompt")}
                </Label>
                <Textarea
                  id="vision-prompt"
                  value={config.visionBridge.prompt}
                  onChange={(e) =>
                    handleVisionBridgeChange({ prompt: e.target.value })
                  }
                />
              </div>
            </div>
          )}
        </div>
      </div>

      <div className="border-t pt-6 mt-6">
        <div className="space-y-1 mb-4">
          <h3 className="text-sm font-medium">
            {t("settings.advanced.optimizer.title")}
          </h3>
          <p className="text-xs text-muted-foreground">
            {t("settings.advanced.optimizer.description")}
          </p>
        </div>

        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <div className="space-y-0.5">
              <Label>{t("settings.advanced.optimizer.enabled")}</Label>
            </div>
            <Switch
              checked={optimizerConfig.enabled}
              onCheckedChange={(checked) =>
                handleOptimizerChange({ enabled: checked })
              }
            />
          </div>

          <div className="space-y-4 pl-4">
            <div className="flex items-center justify-between">
              <div className="space-y-0.5">
                <Label>
                  {t("settings.advanced.optimizer.thinkingOptimizer")}
                </Label>
                <p className="text-xs text-muted-foreground">
                  {t(
                    "settings.advanced.optimizer.thinkingOptimizerDescription",
                  )}
                </p>
              </div>
              <Switch
                checked={optimizerConfig.thinkingOptimizer}
                disabled={!optimizerConfig.enabled}
                onCheckedChange={(checked) =>
                  handleOptimizerChange({ thinkingOptimizer: checked })
                }
              />
            </div>

            <div className="flex items-center justify-between">
              <div className="space-y-0.5">
                <Label>{t("settings.advanced.optimizer.cacheInjection")}</Label>
                <p className="text-xs text-muted-foreground">
                  {t("settings.advanced.optimizer.cacheInjectionDescription")}
                </p>
              </div>
              <Switch
                checked={optimizerConfig.cacheInjection}
                disabled={!optimizerConfig.enabled}
                onCheckedChange={(checked) =>
                  handleOptimizerChange({ cacheInjection: checked })
                }
              />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
