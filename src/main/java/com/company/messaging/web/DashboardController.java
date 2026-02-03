package com.company.messaging.web;

import com.company.messaging.core.diagnostics.DiagnosticService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
@RequiredArgsConstructor
public class DashboardController {
    
    private final DiagnosticService diagnosticService;
    
    @GetMapping("/dashboard")
    public String dashboard(Model model) {
        model.addAttribute("healthInfo", diagnosticService.getDiagnosticInfo());
        model.addAttribute("uptime", formatUptime(
            ManagementFactory.getRuntimeMXBean().getUptime()));
        return "dashboard";
    }
    
    @GetMapping("/dashboard/audit")
    public String auditView() {
        return "audit";
    }
    
    @GetMapping("/dashboard/traces")
    public String tracesView() {
        return "traces";
    }
    
    @GetMapping("/dashboard/alerts")
    public String alertsView() {
        return "alerts";
    }
    
    private String formatUptime(long uptimeMs) {
        long days = uptimeMs / (1000 * 60 * 60 * 24);
        long hours = (uptimeMs % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60);
        long minutes = (uptimeMs % (1000 * 60 * 60)) / (1000 * 60);
        return String.format("%dd %dh %dm", days, hours, minutes);
    }
}
