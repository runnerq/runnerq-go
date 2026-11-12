//go:build go1.27

package runnerq

// The method forms of the generic helpers need generic methods, which Go
// 1.27 introduced. On older toolchains use the package functions RunStep and
// Activity; both forms are the same call.

// RunStep is the method form of the package function RunStep:
//
//	receipt, err := ctx.RunStep("charge", func(c context.Context) (Receipt, error) {
//	    return payments.Charge(c, orderID, amount)
//	})
func (c ActivityContext) RunStep[R any](name string, fn Step[R]) (R, error) {
	return RunStep(c, name, fn)
}

// Activity is the method form of the package function Activity:
//
//	fut, err := ctx.ActivityExecutor.Activity[ResizeImage]().Payload(p).Execute(ctx.Ctx)
func (w *ActivityExecutor) Activity[T any]() *ActivityBuilder {
	return Activity[T](w)
}
