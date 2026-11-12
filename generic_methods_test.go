//go:build go1.27

package runnerq

import (
	"context"
	"testing"
)

func TestGenericMethodsMatchPackageFunctions(t *testing.T) {
	exec := &ActivityExecutor{}
	if got, want := exec.Activity[namedHandler]().activityType, Activity[namedHandler](exec).activityType; got != want {
		t.Fatalf("method Activity = %q, package Activity = %q", got, want)
	}
	n, err := ActivityContext{Ctx: context.Background()}.RunStep("n", func(context.Context) (int, error) { return 7, nil })
	if err != nil || n != 7 {
		t.Fatalf("RunStep method = %d, %v; want 7", n, err)
	}
}
