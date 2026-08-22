//go:build e2e
// +build e2e

/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package e2e

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/zevlo/klease/test/utils"
)

// namespace where the project is deployed in
const namespace = "klease-system"

// serviceAccountName created for the project
const serviceAccountName = "klease-controller-manager"

// metricsServiceName is the name of the metrics service of the project
const metricsServiceName = "klease-controller-manager-metrics-service"

// metricsRoleBindingName is the name of the RBAC that will be created to allow get the metrics data
const metricsRoleBindingName = "klease-metrics-binding"

var _ = Describe("Manager", Ordered, func() {
	var controllerPodName string

	// Before running the tests, set up the environment by creating the namespace,
	// enforce the restricted security policy to the namespace, installing CRDs,
	// and deploying the controller.
	BeforeAll(func() {
		By("creating manager namespace")
		cmd := exec.Command("kubectl", "create", "ns", namespace)
		_, err := utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to create namespace")

		By("labeling the namespace to enforce the restricted security policy")
		cmd = exec.Command("kubectl", "label", "--overwrite", "ns", namespace,
			"pod-security.kubernetes.io/enforce=restricted")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to label namespace with restricted policy")

		By("installing CRDs")
		cmd = exec.Command("make", "install")
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to install CRDs")

		By("deploying the controller-manager")
		cmd = exec.Command("make", "deploy", fmt.Sprintf("IMG=%s", managerImage))
		_, err = utils.Run(cmd)
		Expect(err).NotTo(HaveOccurred(), "Failed to deploy the controller-manager")
	})

	// After all tests have been executed, clean up by undeploying the controller, uninstalling CRDs,
	// and deleting the namespace.
	AfterAll(func() {
		By("cleaning up the curl pod for metrics")
		cmd := exec.Command("kubectl", "delete", "pod", "curl-metrics", "-n", namespace)
		_, _ = utils.Run(cmd)

		By("undeploying the controller-manager")
		cmd = exec.Command("make", "undeploy")
		_, _ = utils.Run(cmd)

		By("uninstalling CRDs")
		cmd = exec.Command("make", "uninstall")
		_, _ = utils.Run(cmd)

		By("removing manager namespace")
		cmd = exec.Command("kubectl", "delete", "ns", namespace)
		_, _ = utils.Run(cmd)
	})

	// After each test, check for failures and collect logs, events,
	// and pod descriptions for debugging.
	AfterEach(func() {
		specReport := CurrentSpecReport()
		if specReport.Failed() {
			By("Fetching controller manager pod logs")
			cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
			controllerLogs, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Controller logs:\n %s", controllerLogs)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Controller logs: %s", err)
			}

			By("Fetching Kubernetes events")
			cmd = exec.Command("kubectl", "get", "events", "-n", namespace, "--sort-by=.lastTimestamp")
			eventsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Kubernetes events:\n%s", eventsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get Kubernetes events: %s", err)
			}

			By("Fetching curl-metrics logs")
			cmd = exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
			metricsOutput, err := utils.Run(cmd)
			if err == nil {
				_, _ = fmt.Fprintf(GinkgoWriter, "Metrics logs:\n %s", metricsOutput)
			} else {
				_, _ = fmt.Fprintf(GinkgoWriter, "Failed to get curl-metrics logs: %s", err)
			}

			By("Fetching controller manager pod description")
			cmd = exec.Command("kubectl", "describe", "pod", controllerPodName, "-n", namespace)
			podDescription, err := utils.Run(cmd)
			if err == nil {
				fmt.Println("Pod description:\n", podDescription)
			} else {
				fmt.Println("Failed to describe controller pod")
			}
		}
	})

	SetDefaultEventuallyTimeout(2 * time.Minute)
	SetDefaultEventuallyPollingInterval(time.Second)

	Context("Manager", func() {
		It("should run successfully", func() {
			By("validating that the controller-manager pod is running as expected")
			verifyControllerUp := func(g Gomega) {
				By("getting the name of the controller-manager pod")
				cmd := exec.Command("kubectl", "get",
					"pods", "-l", "control-plane=controller-manager",
					"-o", "go-template={{ range .items }}"+
						"{{ if not .metadata.deletionTimestamp }}"+
						"{{ .metadata.name }}"+
						"{{ \"\\n\" }}{{ end }}{{ end }}",
					"-n", namespace,
				)

				podOutput, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve controller-manager pod information")
				podNames := utils.GetNonEmptyLines(podOutput)
				g.Expect(podNames).To(HaveLen(1), "expected 1 controller pod running")
				controllerPodName = podNames[0]
				g.Expect(controllerPodName).To(ContainSubstring("controller-manager"))

				By("validating the pod's status")
				cmd = exec.Command("kubectl", "get",
					"pods", controllerPodName, "-o", "jsonpath={.status.phase}",
					"-n", namespace,
				)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Running"), "Incorrect controller-manager pod status")
			}
			Eventually(verifyControllerUp).Should(Succeed())
		})

		It("should ensure the metrics endpoint is serving metrics", func() {
			By("creating a ClusterRoleBinding for the service account to allow access to metrics")
			cmd := exec.Command("kubectl", "create", "clusterrolebinding", metricsRoleBindingName,
				"--clusterrole=klease-metrics-reader",
				fmt.Sprintf("--serviceaccount=%s:%s", namespace, serviceAccountName),
			)
			_, err := utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create ClusterRoleBinding")

			By("validating that the metrics service is available")
			cmd = exec.Command("kubectl", "get", "service", metricsServiceName, "-n", namespace)
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Metrics service should exist")

			By("getting the service account token")
			token, err := serviceAccountToken()
			Expect(err).NotTo(HaveOccurred())
			Expect(token).NotTo(BeEmpty())

			By("ensuring the controller pod is ready")
			verifyControllerPodReady := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pod", controllerPodName, "-n", namespace,
					"-o", "jsonpath={.status.conditions[?(@.type=='Ready')].status}")
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("True"), "Controller pod not ready")
			}
			Eventually(verifyControllerPodReady, 3*time.Minute, time.Second).Should(Succeed())

			By("verifying that the controller manager is serving the metrics server")
			verifyMetricsServerStarted := func(g Gomega) {
				cmd := exec.Command("kubectl", "logs", controllerPodName, "-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(ContainSubstring("Serving metrics server"),
					"Metrics server not yet started")
			}
			Eventually(verifyMetricsServerStarted, 3*time.Minute, time.Second).Should(Succeed())

			// +kubebuilder:scaffold:e2e-metrics-webhooks-readiness

			By("creating the curl-metrics pod to access the metrics endpoint")
			cmd = exec.Command("kubectl", "run", "curl-metrics", "--restart=Never",
				"--namespace", namespace,
				"--image=curlimages/curl:latest",
				"--overrides",
				fmt.Sprintf(`{
					"spec": {
						"containers": [{
							"name": "curl",
							"image": "curlimages/curl:latest",
							"command": ["/bin/sh", "-c"],
							"args": [
								"for i in $(seq 1 30); do curl -v -k -H 'Authorization: Bearer %s' https://%s.%s.svc.cluster.local:8443/metrics && exit 0 || sleep 2; done; exit 1"
							],
							"securityContext": {
								"readOnlyRootFilesystem": true,
								"allowPrivilegeEscalation": false,
								"capabilities": {
									"drop": ["ALL"]
								},
								"runAsNonRoot": true,
								"runAsUser": 1000,
								"seccompProfile": {
									"type": "RuntimeDefault"
								}
							}
						}],
						"serviceAccountName": "%s"
					}
				}`, token, metricsServiceName, namespace, serviceAccountName))
			_, err = utils.Run(cmd)
			Expect(err).NotTo(HaveOccurred(), "Failed to create curl-metrics pod")

			By("waiting for the curl-metrics pod to complete.")
			verifyCurlUp := func(g Gomega) {
				cmd := exec.Command("kubectl", "get", "pods", "curl-metrics",
					"-o", "jsonpath={.status.phase}",
					"-n", namespace)
				output, err := utils.Run(cmd)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(output).To(Equal("Succeeded"), "curl pod in wrong status")
			}
			Eventually(verifyCurlUp, 5*time.Minute).Should(Succeed())

			By("getting the metrics by checking curl-metrics logs")
			verifyMetricsAvailable := func(g Gomega) {
				metricsOutput, err := getMetricsOutput()
				g.Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
				g.Expect(metricsOutput).NotTo(BeEmpty())
				g.Expect(metricsOutput).To(ContainSubstring("< HTTP/1.1 200 OK"))
				// klease's custom queue metrics must be served alongside
				// the generic controller-runtime ones.
				g.Expect(metricsOutput).To(ContainSubstring("klease_queue_depth"))
			}
			Eventually(verifyMetricsAvailable, 2*time.Minute).Should(Succeed())
		})

		// +kubebuilder:scaffold:e2e-webhooks-checks

		// TODO: Customize the e2e test suite with scenarios specific to your project.
		// Consider applying sample/CR(s) and check their status and/or verifying
		// the reconciliation by using the metrics, i.e.:
		// metricsOutput, err := getMetricsOutput()
		// Expect(err).NotTo(HaveOccurred(), "Failed to retrieve logs from curl pod")
		// Expect(metricsOutput).To(ContainSubstring(
		//    fmt.Sprintf(`controller_runtime_reconcile_total{controller="%s",result="success"} 1`,
		//    strings.ToLower(<Kind>),
		// ))
	})

	// The lifecycle specs validate the full GPULease state machine against
	// a real kubelet: pods are created by the Deployment controller and
	// terminate on their own grace period (they ignore SIGTERM for ~15s),
	// so admission, drain, and strict handoff are observed end-to-end.
	Context("GPULease lifecycle", func() {
		const (
			leaseNamespace    = "klease-e2e"
			leaseA            = "lease-a"
			leaseB            = "lease-b"
			workloadA         = "workload-a"
			workloadB         = "workload-b"
			stateActive       = "Active"
			statePending      = "Pending"
			stateDraining     = "Draining"
			stateExpired      = "Expired"
			workloadsManifest = "test/e2e/testdata/workloads.yaml"
			leaseAManifest    = "test/e2e/testdata/lease-a.yaml"
			leaseBManifest    = "test/e2e/testdata/lease-b.yaml"
			eventuallyTimeout = 3 * time.Minute
			eventuallyPolling = 2 * time.Second
			replicaJSONPath   = "-o=jsonpath={.spec.replicas}"
			deletionJSONPath  = "-o=jsonpath={.metadata.deletionTimestamp}"
			podPhaseTemplate  = "-o=go-template={{range .items}}{{.status.phase}}{{end}}"
			podCountTemplate  = "-o=go-template={{len .items}}"
		)

		kubectl := func(args ...string) (string, error) {
			return utils.Run(exec.Command("kubectl", args...))
		}
		leaseState := func(name string) string {
			out, _ := kubectl("get", "gpulease", name, "-n", leaseNamespace, "-o=jsonpath={.status.state}")
			return strings.TrimSpace(out)
		}
		deploymentReplicas := func(name string) string {
			out, _ := kubectl("get", "deployment", name, "-n", leaseNamespace, replicaJSONPath)
			return strings.TrimSpace(out)
		}
		podPhase := func(workload string) string {
			out, _ := kubectl("get", "pods", "-n", leaseNamespace, "-l", "app="+workload, podPhaseTemplate)
			return strings.TrimSpace(out)
		}
		podCount := func(workload string) string {
			out, _ := kubectl("get", "pods", "-n", leaseNamespace, "-l", "app="+workload, podCountTemplate)
			return strings.TrimSpace(out)
		}

		BeforeAll(func() {
			By("applying the managed workloads")
			_, err := kubectl("apply", "-f", workloadsManifest)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply e2e workloads")

			By("waiting for both Deployments to be observed")
			for _, name := range []string{workloadA, workloadB} {
				Eventually(func(g Gomega) {
					out, err := kubectl("get", "deployment", name, "-n", leaseNamespace)
					g.Expect(err).NotTo(HaveOccurred())
					g.Expect(out).To(ContainSubstring(name))
				}, eventuallyTimeout, eventuallyPolling).Should(Succeed())
			}
		})

		AfterAll(func() {
			By("removing the e2e fixtures namespace")
			_, _ = kubectl("delete", "namespace", leaseNamespace, "--ignore-not-found")
		})

		It("admits the first lease, runs one pod, and queues the second", func() {
			By("creating lease-a; it is admitted immediately and scales workload-a to 1")
			_, err := kubectl("apply", "-f", leaseAManifest)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply lease-a")
			Eventually(func(g Gomega) {
				g.Expect(leaseState(leaseA)).To(Equal(stateActive))
				g.Expect(deploymentReplicas(workloadA)).To(Equal("1"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())

			By("creating lease-b; it queues at position 0 while workload-b stays at 0")
			_, err = kubectl("apply", "-f", leaseBManifest)
			Expect(err).NotTo(HaveOccurred(), "Failed to apply lease-b")
			Eventually(func(g Gomega) {
				g.Expect(leaseState(leaseB)).To(Equal(statePending))
				pos, err := kubectl("get", "gpulease", leaseB, "-n", leaseNamespace, "-o=jsonpath={.status.queuePosition}")
				g.Expect(err).NotTo(HaveOccurred())
				// queuePosition is omitempty in the API: the head's 0
				// serializes as absent, so accept empty as well as "0".
				g.Expect(strings.TrimSpace(pos)).To(SatisfyAny(Equal("0"), BeEmpty()))
				g.Expect(deploymentReplicas(workloadB)).To(Equal("0"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())
		})

		It("drains the holder at expiry and admits the successor only after the drain completes", func() {
			By("waiting for the holder's pod to run")
			Eventually(func(g Gomega) {
				g.Expect(podPhase(workloadA)).To(Equal("Running"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())

			By("observing the strict handoff: lease-a Draining, workload-a at 0, lease-b still Pending")
			// The pod ignores SIGTERM for ~15s, so this window is stable
			// enough to sample all three facts in one pass.
			Eventually(func(g Gomega) {
				g.Expect(leaseState(leaseA)).To(Equal(stateDraining))
				g.Expect(leaseState(leaseB)).To(Equal(statePending))
				g.Expect(deploymentReplicas(workloadA)).To(Equal("0"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())

			By("after the drain: lease-a Expired, lease-b Active, workload-b runs the pod")
			Eventually(func(g Gomega) {
				g.Expect(leaseState(leaseA)).To(Equal(stateExpired))
				g.Expect(leaseState(leaseB)).To(Equal(stateActive))
				g.Expect(deploymentReplicas(workloadB)).To(Equal("1"))
				g.Expect(podPhase(workloadB)).To(Equal("Running"))
				g.Expect(podCount(workloadA)).To(Equal("0"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())
		})

		It("holds a deleted holder until its workload drains, then releases it", func() {
			By("deleting lease-b mid-slot")
			// --wait=false: kubectl would otherwise block until the
			// finalizer releases the object, hiding the drain window.
			_, err := kubectl("delete", "gpulease", leaseB, "-n", leaseNamespace, "--wait=false")
			Expect(err).NotTo(HaveOccurred(), "Failed to delete lease-b")

			By("the object persists in Draining under the finalizer while its pod terminates")
			Eventually(func(g Gomega) {
				delTs, err := kubectl("get", "gpulease", leaseB, "-n", leaseNamespace, deletionJSONPath)
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(strings.TrimSpace(delTs)).NotTo(BeEmpty())
				g.Expect(leaseState(leaseB)).To(Equal(stateDraining))
				g.Expect(deploymentReplicas(workloadB)).To(Equal("0"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())

			By("once drained, the object is released and the workload stays parked")
			Eventually(func(g Gomega) {
				_, err := kubectl("get", "gpulease", leaseB, "-n", leaseNamespace)
				g.Expect(err).To(HaveOccurred(), "lease-b should be gone once the drain completes")
				g.Expect(deploymentReplicas(workloadB)).To(Equal("0"))
				g.Expect(podCount(workloadB)).To(Equal("0"))
			}, eventuallyTimeout, eventuallyPolling).Should(Succeed())
		})
	})
})

// serviceAccountToken returns a token for the specified service account in the given namespace.
// It uses the Kubernetes TokenRequest API to generate a token by directly sending a request
// and parsing the resulting token from the API response.
func serviceAccountToken() (string, error) {
	const tokenRequestRawString = `{
		"apiVersion": "authentication.k8s.io/v1",
		"kind": "TokenRequest"
	}`

	By("creating temporary file to store the token request")
	secretName := fmt.Sprintf("%s-token-request", serviceAccountName)
	tokenRequestFile := filepath.Join("/tmp", secretName)
	err := os.WriteFile(tokenRequestFile, []byte(tokenRequestRawString), os.FileMode(0o644))
	if err != nil {
		return "", err
	}

	var out string
	verifyTokenCreation := func(g Gomega) {
		By("executing kubectl command to create the token")
		cmd := exec.Command("kubectl", "create", "--raw", fmt.Sprintf(
			"/api/v1/namespaces/%s/serviceaccounts/%s/token",
			namespace,
			serviceAccountName,
		), "-f", tokenRequestFile)

		output, err := cmd.CombinedOutput()
		g.Expect(err).NotTo(HaveOccurred())

		By("parsing the JSON output to extract the token")
		var token tokenRequest
		err = json.Unmarshal(output, &token)
		g.Expect(err).NotTo(HaveOccurred())

		out = token.Status.Token
	}
	Eventually(verifyTokenCreation).Should(Succeed())

	return out, err
}

// getMetricsOutput retrieves and returns the logs from the curl pod used to access the metrics endpoint.
func getMetricsOutput() (string, error) {
	By("getting the curl-metrics logs")
	cmd := exec.Command("kubectl", "logs", "curl-metrics", "-n", namespace)
	return utils.Run(cmd)
}

// tokenRequest is a simplified representation of the Kubernetes TokenRequest API response,
// containing only the token field that we need to extract.
type tokenRequest struct {
	Status struct {
		Token string `json:"token"`
	} `json:"status"`
}
